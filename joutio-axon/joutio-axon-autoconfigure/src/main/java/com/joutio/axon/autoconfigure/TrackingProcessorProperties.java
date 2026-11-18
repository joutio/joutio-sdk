package com.joutio.axon.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Réglages communs à tous les tracking processors du service ({@code joutio.axon.*}).
 *
 * <p>{@code eventAvailabilityTimeout} (défaut 5 s) : durée pendant laquelle un worker attend un
 * événement avant de prolonger sa réservation de segment ({@code extendClaim}, une transaction).
 * Au repos, chaque worker fait donc une transaction par intervalle : 1 s (défaut Axon) multiplié par
 * le nombre de segments et de groupes saturait le pool JDBC. Aucune latence ajoutée : le worker est
 * réveillé dès qu'un événement arrive. Doit rester sous le {@code claimTimeout} du token store,
 * sinon une autre réplique volerait le segment d'un worker au repos.</p>
 */
@ConfigurationProperties(prefix = "joutio.axon")
public record TrackingProcessorProperties(Duration eventAvailabilityTimeout) {

    static final Duration DEFAULT_EVENT_AVAILABILITY_TIMEOUT = Duration.ofSeconds(5);

    public TrackingProcessorProperties {
        eventAvailabilityTimeout = eventAvailabilityTimeout == null
                ? DEFAULT_EVENT_AVAILABILITY_TIMEOUT
                : eventAvailabilityTimeout;
        if (eventAvailabilityTimeout.isNegative() || eventAvailabilityTimeout.isZero()) {
            throw new IllegalArgumentException("joutio.axon.event-availability-timeout must be > 0");
        }
    }
}
