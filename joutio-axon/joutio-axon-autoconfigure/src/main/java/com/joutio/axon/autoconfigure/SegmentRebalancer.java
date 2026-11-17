package com.joutio.axon.autoconfigure;

import org.axonframework.common.transaction.NoTransactionManager;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.config.Configuration;
import org.axonframework.eventhandling.Segment;
import org.axonframework.eventhandling.TrackingEventProcessor;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.context.SmartLifecycle;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Répartit les segments des groupes segmentés entre les répliques du service.
 *
 * <p>Hors Axon Server, un {@link TrackingEventProcessor} n'équilibre rien : chaque thread réclame
 * le premier segment libre. Chaque réplique a autant de threads que de segments (une réplique
 * seule les traite tous) ; périodiquement, une réplique qui détient plus que sa part
 * ({@code ceil(segments / répliques découvertes)}) en libère l'excédent, qu'une autre réplique
 * réclame au tour suivant. La libération dure deux intervalles pour ne pas reprendre aussitôt le
 * segment libéré.</p>
 *
 * <p>Garde-fou : une réplique découverte n'est pas forcément en état de traiter (démarrage, crash en
 * boucle). Si un segment libéré n'a été repris par personne au tour suivant, les libérations du
 * groupe sont suspendues pendant {@value #SUSPENSION_INTERVALS} intervalles : la réplique garde tout
 * plutôt que de laisser des segments sans propriétaire.</p>
 *
 * <p>Ne touche jamais l'état des événements : une libération attend la fin du lot en cours, et le
 * token reste dans le token store.</p>
 */
public class SegmentRebalancer implements SmartLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(SegmentRebalancer.class);
    static final int SUSPENSION_INTERVALS = 10;

    private final SegmentedProcessingGroupsProperties properties;
    private final Configuration axonConfiguration;
    private final ObjectProvider<DiscoveryClient> discoveryClient;
    private final String serviceId;
    private final Clock clock;
    private final Map<String, Set<Integer>> released = new ConcurrentHashMap<>();
    private final Map<String, Instant> suspendedUntil = new ConcurrentHashMap<>();
    private ScheduledExecutorService executor;

    public SegmentRebalancer(SegmentedProcessingGroupsProperties properties,
                             Configuration axonConfiguration,
                             ObjectProvider<DiscoveryClient> discoveryClient,
                             String serviceId) {
        this(properties, axonConfiguration, discoveryClient, serviceId, Clock.systemUTC());
    }

    SegmentRebalancer(SegmentedProcessingGroupsProperties properties,
                      Configuration axonConfiguration,
                      ObjectProvider<DiscoveryClient> discoveryClient,
                      String serviceId,
                      Clock clock) {
        this.properties = properties;
        this.axonConfiguration = axonConfiguration;
        this.discoveryClient = discoveryClient;
        this.serviceId = serviceId;
        this.clock = clock;
    }

    /** Segments à libérer pour revenir à sa part ; jamais négatif, au moins une réplique comptée. */
    static int segmentsToRelease(int owned, int totalSegments, int instances) {
        int share = (totalSegments + Math.max(1, instances) - 1) / Math.max(1, instances);
        return Math.max(0, owned - share);
    }

    void rebalance() {
        int instances = instances();
        properties.processors().keySet().forEach(group -> axonConfiguration.eventProcessingConfiguration()
                .eventProcessor(group, TrackingEventProcessor.class)
                .filter(TrackingEventProcessor::isRunning)
                .ifPresent(processor -> {
                    try {
                        rebalance(group, processor, instances);
                    } catch (RuntimeException e) {
                        logger.warn("Rééquilibrage en échec: groupe={} ({})", group, e.getMessage());
                    }
                }));
    }

    private void rebalance(String group, TrackingEventProcessor processor, int instances) {
        Instant now = clock.instant();
        Instant suspension = suspendedUntil.get(group);
        if (suspension != null && now.isBefore(suspension)) {
            return;
        }
        TokenStore tokenStore = axonConfiguration.eventProcessingConfiguration().tokenStore(group);
        TransactionManager transactionManager = axonConfiguration.getComponent(
                TransactionManager.class, NoTransactionManager::instance);
        if (transactionManager.fetchInTransaction(() -> releasedButUnclaimed(group, tokenStore))) {
            suspendedUntil.put(group, now.plus(properties.rebalanceInterval().multipliedBy(SUSPENSION_INTERVALS)));
            logger.warn("Rééquilibrage suspendu: groupe={}, segments libérés repris par aucune autre réplique "
                    + "({} répliques découvertes)", group, instances);
            return;
        }
        List<Integer> owned = List.copyOf(processor.processingStatus().keySet());
        int total = transactionManager.fetchInTransaction(() -> tokenStore.fetchSegments(group)).length;
        int excess = segmentsToRelease(owned.size(), total, instances);
        if (excess == 0) {
            return;
        }
        long releaseMillis = properties.rebalanceInterval().multipliedBy(2).toMillis();
        Set<Integer> releasing = owned.stream().limit(excess).collect(Collectors.toSet());
        released.put(group, releasing);
        releasing.forEach(segment -> {
            logger.info("Rééquilibrage: groupe={}, segment={} libéré ({} détenus, {} segments, {} répliques)",
                    group, segment, owned.size(), total, instances);
            processor.releaseSegment(segment, releaseMillis, TimeUnit.MILLISECONDS);
        });
    }

    /**
     * Vrai si un segment libéré au tour précédent est encore sans propriétaire. Lit les tokens :
     * à appeler dans une transaction (le token JPA est un LOB).
     */
    private boolean releasedButUnclaimed(String group, TokenStore tokenStore) {
        Set<Integer> previous = released.remove(group);
        if (previous == null || previous.isEmpty()) {
            return false;
        }
        return tokenStore.fetchAvailableSegments(group).stream()
                .map(Segment::getSegmentId)
                .anyMatch(previous::contains);
    }

    private int instances() {
        DiscoveryClient client = discoveryClient.getIfAvailable();
        return client == null ? 1 : client.getInstances(serviceId).size();
    }

    @Override
    public void start() {
        if (properties.processors().isEmpty()) {
            return;
        }
        long intervalMillis = properties.rebalanceInterval().toMillis();
        executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "axon-segment-rebalancer");
            thread.setDaemon(true);
            return thread;
        });
        executor.scheduleWithFixedDelay(this::safeRebalance, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    private void safeRebalance() {
        try {
            rebalance();
        } catch (RuntimeException e) {
            logger.warn("Rééquilibrage des segments en échec: {}", e.getMessage());
        }
    }

    @Override
    public void stop() {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    @Override
    public boolean isRunning() {
        return executor != null;
    }
}
