package com.joutio.axon.grpc.query;

import java.time.Duration;

/** Mesures du bus de requêtes gRPC, implémentées avec Micrometer par l'auto-configuration. */
public interface QueryBusMetrics {

    QueryBusMetrics NONE = new QueryBusMetrics() {
        @Override
        public void queryCompleted(String queryName, Outcome outcome, Duration duration) {
        }

        @Override
        public void updateReceived(String queryName) {
        }
    };

    /** Requête point à point envoyée à un pair, avec son issue. */
    void queryCompleted(String queryName, Outcome outcome, Duration duration);

    /** Update reçu par un abonné de ce pod. */
    void updateReceived(String queryName);

    enum Outcome {
        SUCCESS, ERROR, TIMEOUT
    }
}
