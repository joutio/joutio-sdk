package com.joutio.axon.grpc;

import org.springframework.cloud.client.ServiceInstance;

import java.util.Optional;

/**
 * Adresse gRPC d'un membre du bus : hôte de l'instance découverte et port nommé {@code grpc}.
 *
 * <p>En Kubernetes, Spring Cloud Kubernetes publie chaque port nommé du Service dans les métadonnées
 * de l'instance ({@code port.<nom>}). En local, la même clé est déclarée dans
 * {@code spring.cloud.discovery.client.simple.instances}.</p>
 */
public record GrpcEndpoint(String host, int port) {

    /** Clé de métadonnée du port gRPC d'une instance découverte. */
    public static final String PORT_METADATA = "port.grpc";

    /** Adresse gRPC d'une instance découverte, vide si elle n'annonce pas de port {@code grpc}. */
    public static Optional<GrpcEndpoint> of(ServiceInstance instance) {
        String port = instance.getMetadata() == null ? null : instance.getMetadata().get(PORT_METADATA);
        if (port == null || port.isBlank()) {
            return Optional.empty();
        }
        return Optional.of(new GrpcEndpoint(instance.getHost(), Integer.parseInt(port.trim())));
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}
