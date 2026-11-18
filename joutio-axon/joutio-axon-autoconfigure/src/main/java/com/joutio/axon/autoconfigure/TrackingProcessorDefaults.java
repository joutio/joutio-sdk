package com.joutio.axon.autoconfigure;

import org.axonframework.eventhandling.TrackingEventProcessorConfiguration;
import org.axonframework.messaging.StreamableMessageSource;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Applique les réglages communs du SDK à une configuration de tracking processor.
 *
 * <p>Toute configuration enregistrée par nom remplace entièrement la configuration par défaut :
 * chaque enregistrement du SDK (défaut, groupes segmentés, groupes instance-scopés) passe donc par
 * {@link #apply(String, TrackingEventProcessorConfiguration)}.</p>
 *
 * <p>Sagas : sans configuration propre, Axon démarre un groupe de saga à la <b>tête</b> du flux.
 * Déclarer une configuration (par défaut ou par nom) retire ce comportement ; il est rétabli ici
 * pour les groupes de saga connus.</p>
 */
public class TrackingProcessorDefaults {

    private final Duration eventAvailabilityTimeout;
    private final Set<String> sagaGroups;

    public TrackingProcessorDefaults(Duration eventAvailabilityTimeout, Set<String> sagaGroups) {
        this.eventAvailabilityTimeout = eventAvailabilityTimeout;
        this.sagaGroups = Set.copyOf(sagaGroups);
    }

    public Set<String> sagaGroups() {
        return sagaGroups;
    }

    /** Configuration d'un groupe sans réglage propre : un thread, réglages communs. */
    public TrackingEventProcessorConfiguration defaultConfiguration() {
        return withTimeout(TrackingEventProcessorConfiguration.forSingleThreadedProcessing());
    }

    /** Réglages communs, plus le token initial en tête si {@code group} est un groupe de saga. */
    public TrackingEventProcessorConfiguration apply(String group, TrackingEventProcessorConfiguration configuration) {
        TrackingEventProcessorConfiguration result = withTimeout(configuration);
        return sagaGroups.contains(group)
                ? result.andInitialTrackingToken(StreamableMessageSource::createHeadToken)
                : result;
    }

    private TrackingEventProcessorConfiguration withTimeout(TrackingEventProcessorConfiguration configuration) {
        return configuration.andEventAvailabilityTimeout(eventAvailabilityTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }
}
