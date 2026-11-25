package com.joutio.axon.autoconfigure.metrics;

import com.joutio.axon.grpc.query.QueryBusMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.util.Locale;

/**
 * Mesures du bus de requêtes gRPC : timer {@code joutio.axon.grpc.query} (requête envoyée à un pair, par nom et
 * issue) et compteur {@code joutio.axon.subscription.updates} (updates reçus par les abonnés du pod, par nom). Les
 * compteurs naissent au premier usage, après le démarrage d'Axon.
 */
public class MicrometerQueryBusMetrics implements QueryBusMetrics {

    private final MeterRegistry registry;

    public MicrometerQueryBusMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void queryCompleted(String queryName, Outcome outcome, Duration duration) {
        Timer.builder("joutio.axon.grpc.query")
                .description("Requêtes envoyées à un pair par le bus gRPC")
                .tag("query", queryName)
                .tag("outcome", outcome.name().toLowerCase(Locale.ROOT))
                .register(registry)
                .record(duration);
    }

    @Override
    public void updateReceived(String queryName) {
        Counter.builder("joutio.axon.subscription.updates")
                .description("Updates de vues vivantes reçus par les abonnés de ce pod")
                .tag("query", queryName)
                .register(registry)
                .increment();
    }
}
