package com.joutio.axon.grpc.membership;

import com.joutio.axon.grpc.GrpcEndpoint;
import com.joutio.axon.grpc.LocalMember;
import com.joutio.axon.grpc.PeerChannels;
import com.joutio.axon.grpc.membership.proto.CapabilitiesRequest;
import com.joutio.axon.grpc.membership.proto.MemberCapabilities;
import com.joutio.axon.grpc.membership.proto.MembershipServiceGrpc;
import io.axoniq.axonserver.grpc.command.CommandSubscription;
import io.axoniq.axonserver.grpc.query.QuerySubscription;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.context.SmartLifecycle;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Membres du bus gRPC et leurs capacités, vus par le pod local.
 *
 * <p>Les pairs viennent du {@link DiscoveryClient} (filtre {@code component=axon} en Kubernetes,
 * instances simples en local) ; seuls ceux qui annoncent un port {@code grpc} sont retenus. À chaque
 * rafraîchissement, le pod appelle {@code Capabilities} sur chacun, en parallèle et avec un délai.</p>
 *
 * <p>Un pair encore découvert dont la lecture échoue garde ses dernières capacités pendant
 * {@code retention} (pod qui démarre, identity qui redémarre et ne valide plus le jeton) : sinon chaque
 * émetteur l'exclurait de son côté, et deux émetteurs n'auraient plus le même anneau de routage. Un
 * pair qui n'est plus découvert, ou qui annonce son départ, est retiré aussitôt.</p>
 *
 * <p>Le pod local figure dans la topologie quand la découverte l'annonce, comme ses pairs le voient ;
 * ses capacités sont lues en mémoire.</p>
 */
public class Membership implements SmartLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(Membership.class);

    private final LocalMember localMember;
    private final LocalCapabilities localCapabilities;
    private final DiscoveryClient discoveryClient;
    private final PeerChannels channels;
    private final Duration refreshInterval;
    private final Duration callTimeout;
    private final Duration retention;
    private final Clock clock;
    private final List<TopologyListener> listeners = new CopyOnWriteArrayList<>();
    private final Map<GrpcEndpoint, Member> lastKnown = new ConcurrentHashMap<>();

    private volatile Topology topology = Topology.EMPTY;
    private volatile ScheduledExecutorService scheduler;

    public Membership(LocalMember localMember,
                      LocalCapabilities localCapabilities,
                      DiscoveryClient discoveryClient,
                      PeerChannels channels,
                      Duration refreshInterval,
                      Duration callTimeout,
                      Duration retention,
                      Clock clock) {
        this.localMember = localMember;
        this.localCapabilities = localCapabilities;
        this.discoveryClient = discoveryClient;
        this.channels = channels;
        this.refreshInterval = refreshInterval;
        this.callTimeout = callTimeout;
        this.retention = retention;
        this.clock = clock;
    }

    public Topology topology() {
        return topology;
    }

    public LocalMember localMember() {
        return localMember;
    }

    public void addListener(TopologyListener listener) {
        listeners.add(listener);
        listener.onTopologyChanged(topology);
    }

    /** Demande un rafraîchissement hors cycle (changement de découverte), sans bloquer l'appelant. */
    public void refreshSoon() {
        ScheduledExecutorService current = scheduler;
        if (current != null && !current.isShutdown()) {
            current.execute(this::refreshSafely);
        }
    }

    /** Relit la découverte et les capacités de chaque pair, puis publie la topologie si elle a changé. */
    public synchronized void refresh() {
        Set<GrpcEndpoint> discovered = discover();
        Set<GrpcEndpoint> peers = discovered.stream()
                .filter(endpoint -> !endpoint.equals(localMember.endpoint()))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        channels.retain(peers);
        lastKnown.keySet().retainAll(peers);

        Map<GrpcEndpoint, CompletableFuture<MemberCapabilities>> calls = new HashMap<>();
        peers.forEach(endpoint -> calls.put(endpoint, capabilitiesOf(endpoint)));
        CompletableFuture.allOf(calls.values().toArray(CompletableFuture[]::new))
                .exceptionally(failure -> null)
                .join();

        Instant now = clock.instant();
        List<Member> members = new ArrayList<>();
        if (discovered.contains(localMember.endpoint())) {
            members.add(localSelf(now));
        }
        calls.forEach((endpoint, call) -> remoteMember(endpoint, call, now).ifPresent(members::add));
        publish(new Topology(members));
    }

    private Set<GrpcEndpoint> discover() {
        Set<GrpcEndpoint> endpoints = new LinkedHashSet<>();
        for (String service : discoveryClient.getServices()) {
            discoveryClient.getInstances(service).forEach(instance -> GrpcEndpoint.of(instance).ifPresentOrElse(
                    endpoints::add,
                    () -> logger.debug("Instance {} of {} has no grpc port, ignored", instance.getUri(), service)));
        }
        return endpoints;
    }

    private CompletableFuture<MemberCapabilities> capabilitiesOf(GrpcEndpoint endpoint) {
        CompletableFuture<MemberCapabilities> result = new CompletableFuture<>();
        MembershipServiceGrpc.newStub(channels.channel(endpoint))
                .withDeadlineAfter(callTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .capabilities(CapabilitiesRequest.newBuilder()
                                .setClientId(localMember.clientId())
                                .setComponentName(localMember.componentName())
                                .build(),
                        new StreamObserver<>() {
                            @Override
                            public void onNext(MemberCapabilities value) {
                                result.complete(value);
                            }

                            @Override
                            public void onError(Throwable t) {
                                result.completeExceptionally(t);
                            }

                            @Override
                            public void onCompleted() {
                                result.completeExceptionally(new IllegalStateException("no capabilities returned"));
                            }
                        });
        return result;
    }

    private Member localSelf(Instant now) {
        return new Member(localMember.endpoint(), localMember.clientId(), localMember.componentName(), true,
                localCapabilities.loadFactor(), localCapabilities.commandNames(), localCapabilities.queryNames(), now);
    }

    private Optional<Member> remoteMember(GrpcEndpoint endpoint,
                                          CompletableFuture<MemberCapabilities> call,
                                          Instant now) {
        MemberCapabilities capabilities;
        try {
            capabilities = call.join();
        } catch (RuntimeException e) {
            return retained(endpoint, now, e.getCause() == null ? e : e.getCause());
        }
        if (capabilities.getLeaving()) {
            if (lastKnown.remove(endpoint) != null) {
                logger.info("gRPC member {} ({}) is leaving", capabilities.getClientId(), endpoint);
            }
            return Optional.empty();
        }
        Member member = new Member(endpoint,
                capabilities.getClientId(),
                capabilities.getComponentName(),
                false,
                capabilities.getCommandsList().stream().mapToInt(CommandSubscription::getLoadFactor).findFirst()
                        .orElse(0),
                capabilities.getCommandsList().stream().map(CommandSubscription::getCommand).collect(Collectors.toSet()),
                capabilities.getQueriesList().stream().map(QuerySubscription::getQuery).collect(Collectors.toSet()),
                now);
        lastKnown.put(endpoint, member);
        return Optional.of(member);
    }

    private Optional<Member> retained(GrpcEndpoint endpoint, Instant now, Throwable failure) {
        Member last = lastKnown.get(endpoint);
        if (last != null && now.isBefore(last.seenAt().plus(retention))) {
            logger.debug("Capabilities of {} unreadable, last known kept ({})", endpoint, failure.getMessage());
            return Optional.of(last);
        }
        if (lastKnown.remove(endpoint) != null) {
            logger.info("Capabilities of {} unreadable for more than {}, member removed ({})",
                    endpoint, retention, failure.getMessage());
        } else {
            logger.debug("Capabilities of {} unreadable ({})", endpoint, failure.getMessage());
        }
        return Optional.empty();
    }

    private void publish(Topology next) {
        if (next.sameAs(topology)) {
            topology = next;
            return;
        }
        topology = next;
        logger.info("gRPC bus members: {}", next.members().stream()
                .map(member -> "%s (%s, %d commands, %d queries)".formatted(member.clientId(), member.endpoint(),
                        member.commandNames().size(), member.queryNames().size()))
                .toList());
        listeners.forEach(listener -> listener.onTopologyChanged(next));
    }

    private void refreshSafely() {
        try {
            refresh();
        } catch (RuntimeException e) {
            logger.warn("gRPC membership refresh failed: {}", e.getMessage(), e);
        }
    }

    @Override
    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform()
                .name("joutio-grpc-membership")
                .daemon(true)
                .factory());
        scheduler.scheduleWithFixedDelay(this::refreshSafely, 0, refreshInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        ScheduledExecutorService current = scheduler;
        scheduler = null;
        if (current != null) {
            current.shutdownNow();
        }
        channels.close();
    }

    @Override
    public boolean isRunning() {
        return scheduler != null;
    }
}
