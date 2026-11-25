package com.joutio.axon.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Bus gRPC entre pods ({@code joutio.axon.grpc.*}).
 *
 * @param port                  port du serveur gRPC du pod (8124), publié en port nommé {@code grpc}
 * @param clientId              nom du membre ; défaut {@code HOSTNAME} (nom du pod), sinon
 *                              {@code <application>@<hôte>:<port>}
 * @param membershipRefresh     intervalle de relecture des capacités des pairs (10 s)
 * @param capabilitiesTimeout   délai d'un appel {@code Capabilities} (3 s)
 * @param capabilitiesRetention durée pendant laquelle un pair encore découvert mais illisible garde ses
 *                              dernières capacités (2 min)
 * @param shutdownGrace         délai laissé aux appels en cours à l'arrêt (10 s)
 */
@ConfigurationProperties(prefix = "joutio.axon.grpc")
public record AxonGrpcProperties(Integer port,
                                 String clientId,
                                 Duration membershipRefresh,
                                 Duration capabilitiesTimeout,
                                 Duration capabilitiesRetention,
                                 Duration shutdownGrace) {

    public static final int DEFAULT_PORT = 8124;

    public AxonGrpcProperties {
        port = port == null ? DEFAULT_PORT : port;
        membershipRefresh = membershipRefresh == null ? Duration.ofSeconds(10) : membershipRefresh;
        capabilitiesTimeout = capabilitiesTimeout == null ? Duration.ofSeconds(3) : capabilitiesTimeout;
        capabilitiesRetention = capabilitiesRetention == null ? Duration.ofMinutes(2) : capabilitiesRetention;
        shutdownGrace = shutdownGrace == null ? Duration.ofSeconds(10) : shutdownGrace;
    }
}
