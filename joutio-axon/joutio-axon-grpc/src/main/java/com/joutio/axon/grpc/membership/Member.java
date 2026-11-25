package com.joutio.axon.grpc.membership;

import com.joutio.axon.grpc.GrpcEndpoint;

import java.time.Instant;
import java.util.Set;

/**
 * Membre du bus tel que le voit le pod local.
 *
 * @param seenAt dernière lecture réussie de ses capacités (instant de la lecture locale pour le pod lui-même)
 */
public record Member(GrpcEndpoint endpoint,
                     String clientId,
                     String componentName,
                     boolean local,
                     int loadFactor,
                     Set<String> commandNames,
                     Set<String> queryNames,
                     Instant seenAt) {

    public Member {
        commandNames = Set.copyOf(commandNames);
        queryNames = Set.copyOf(queryNames);
    }

    /** Même membre, mêmes capacités, quel que soit l'instant de lecture. */
    boolean sameCapabilities(Member other) {
        return endpoint.equals(other.endpoint)
                && clientId.equals(other.clientId)
                && componentName.equals(other.componentName)
                && local == other.local
                && loadFactor == other.loadFactor
                && commandNames.equals(other.commandNames)
                && queryNames.equals(other.queryNames);
    }
}
