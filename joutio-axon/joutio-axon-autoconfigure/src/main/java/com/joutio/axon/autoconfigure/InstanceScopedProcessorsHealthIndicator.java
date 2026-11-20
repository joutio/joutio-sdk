package com.joutio.axon.autoconfigure;

import org.axonframework.config.Configuration;
import org.axonframework.eventhandling.EventTrackerStatus;
import org.axonframework.eventhandling.StreamingEventProcessor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;

import java.util.List;
import java.util.Map;

/**
 * Readiness des groupes instance-scopés : hors service tant qu'un de ces processors n'a pas
 * rattrapé le flux Kafka.
 *
 * <p>Un pod qui reçoit des sessions temps réel avant que ses groupes instance-scopés lisent le flux
 * pousse ses notifications en retard : jusqu'à 30 s pour un pod BFF ajouté sous charge (campagne de
 * charge #9). Une fois tous les groupes à jour, l'indicateur reste {@code UP} : un retard passager
 * sous charge ne doit pas retirer le pod du service.</p>
 *
 * <p>Sans groupe instance-scopé, toujours {@code UP}. Inclus dans le groupe {@code readiness} par
 * défaut ({@link AxonDistributedEnvironmentPostProcessor}).</p>
 */
public class InstanceScopedProcessorsHealthIndicator extends AbstractHealthIndicator {

    private final ObjectProvider<Configuration> configuration;
    private final List<String> processors;
    private volatile boolean caughtUpOnce;

    public InstanceScopedProcessorsHealthIndicator(ObjectProvider<Configuration> configuration,
                                                   List<String> processors) {
        super("Instance-scoped processors health check failed");
        this.configuration = configuration;
        this.processors = List.copyOf(processors);
        this.caughtUpOnce = processors.isEmpty();
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) {
        if (caughtUpOnce) {
            builder.up();
            return;
        }
        Configuration axon = configuration.getIfAvailable();
        List<String> lagging = processors.stream()
                .filter(name -> axon == null || !caughtUp(axon, name))
                .toList();
        if (lagging.isEmpty()) {
            caughtUpOnce = true;
            builder.up();
        } else {
            builder.outOfService().withDetail("lagging", lagging);
        }
    }

    private static boolean caughtUp(Configuration axon, String name) {
        return axon.eventProcessingConfiguration()
                .eventProcessor(name, StreamingEventProcessor.class)
                .filter(StreamingEventProcessor::isRunning)
                .map(StreamingEventProcessor::processingStatus)
                .filter(status -> !status.isEmpty())
                .map(Map::values)
                .map(statuses -> statuses.stream().allMatch(EventTrackerStatus::isCaughtUp))
                .orElse(false);
    }
}
