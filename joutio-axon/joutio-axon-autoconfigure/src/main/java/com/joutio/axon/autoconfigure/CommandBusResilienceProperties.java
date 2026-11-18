package com.joutio.axon.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Résilience du bus de commandes distribué ({@code joutio.axon.command-bus.*}).
 *
 * <p>{@code maxRetries} (3) et {@code retryBackoff} (100 ms, doublé à chaque essai) : nouvel essai des
 * seuls échecs passagers ({@link TransientCommandRetryScheduler}). {@code capabilitiesRetention}
 * (2 min) : durée pendant laquelle un membre injoignable garde ses dernières capacités connues
 * ({@link LastKnownCapabilitiesDiscoveryMode}).</p>
 */
@ConfigurationProperties(prefix = "joutio.axon.command-bus")
public record CommandBusResilienceProperties(Integer maxRetries, Duration retryBackoff, Duration capabilitiesRetention) {

    public CommandBusResilienceProperties {
        maxRetries = maxRetries == null ? 3 : maxRetries;
        retryBackoff = retryBackoff == null ? Duration.ofMillis(100) : retryBackoff;
        capabilitiesRetention = capabilitiesRetention == null ? Duration.ofMinutes(2) : capabilitiesRetention;
        if (maxRetries < 0) {
            throw new IllegalArgumentException("joutio.axon.command-bus.max-retries must be >= 0");
        }
    }
}
