package com.joutio.axon.grpc;

import com.joutio.axon.grpc.membership.MembershipGrpcService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Serveur gRPC du pod : services du bus et {@code MembershipService}.
 *
 * <p>Arrêt gracieux : le pod annonce son départ ({@code leaving}), n'accepte plus de nouvel appel,
 * laisse finir les appels en cours pendant {@code shutdownGrace}, puis coupe le reste.</p>
 */
public class GrpcServer implements SmartLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(GrpcServer.class);

    private final ServerBuilder<?> builder;
    private final MembershipGrpcService membershipService;
    private final Duration shutdownGrace;
    private volatile Server server;

    public GrpcServer(ServerBuilder<?> builder, MembershipGrpcService membershipService, Duration shutdownGrace) {
        this.builder = builder;
        this.membershipService = membershipService;
        this.shutdownGrace = shutdownGrace;
    }

    @Override
    public void start() {
        try {
            server = builder.build().start();
        } catch (IOException e) {
            throw new UncheckedIOException("Could not start the gRPC server", e);
        }
        logger.info("gRPC server listening on port {}", server.getPort());
    }

    @Override
    public void stop() {
        Server current = server;
        if (current == null) {
            return;
        }
        membershipService.leave();
        current.shutdown();
        try {
            if (!current.awaitTermination(shutdownGrace.toMillis(), TimeUnit.MILLISECONDS)) {
                logger.warn("gRPC calls still running after {}, cancelled", shutdownGrace);
                current.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            current.shutdownNow();
        }
        server = null;
    }

    @Override
    public boolean isRunning() {
        return server != null;
    }

    /** Port d'écoute réel (utile quand le port configuré vaut 0). */
    public int port() {
        return server.getPort();
    }
}
