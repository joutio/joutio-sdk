package com.joutio.axon.grpc;

import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Un {@link ManagedChannel} par pair, ouvert au premier appel et réutilisé (HTTP/2 multiplexé),
 * fermé quand le pair quitte la découverte.
 */
public class PeerChannels implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(PeerChannels.class);

    private final Function<GrpcEndpoint, ManagedChannel> channelFactory;
    private final Map<GrpcEndpoint, ManagedChannel> channels = new ConcurrentHashMap<>();

    public PeerChannels(Function<GrpcEndpoint, ManagedChannel> channelFactory) {
        this.channelFactory = channelFactory;
    }

    public ManagedChannel channel(GrpcEndpoint endpoint) {
        return channels.computeIfAbsent(endpoint, channelFactory);
    }

    /** Ferme les canaux des pairs qui ne sont plus découverts. */
    public void retain(Set<GrpcEndpoint> endpoints) {
        channels.keySet().removeIf(endpoint -> {
            if (endpoints.contains(endpoint)) {
                return false;
            }
            ManagedChannel channel = channels.get(endpoint);
            if (channel != null) {
                logger.debug("Closing gRPC channel to {}", endpoint);
                channel.shutdown();
            }
            return true;
        });
    }

    Set<GrpcEndpoint> endpoints() {
        return Set.copyOf(channels.keySet());
    }

    @Override
    public void close() {
        channels.values().forEach(ManagedChannel::shutdownNow);
        channels.clear();
    }
}
