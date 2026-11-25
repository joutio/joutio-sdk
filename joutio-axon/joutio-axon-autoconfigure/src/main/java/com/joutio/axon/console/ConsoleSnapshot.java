package com.joutio.axon.console;

import java.time.Instant;
import java.util.List;

/**
 * Snapshot d'exploitation poussé par l'agent SDK vers {@code joutio-console}
 * ({@code POST /internal/ingest/snapshot}) et servi par l'API interne.
 *
 * <p>Payload <b>borné</b> (aucun payload d'événement) et <b>versionné</b> ({@link #VERSION}) :
 * le service console doit accepter les anciennes versions d'agent.</p>
 */
public record ConsoleSnapshot(
        String version,
        Instant timestamp,
        String application,
        String applicationVersion,
        String instanceId,
        String address,
        long uptimeSeconds,
        List<ProcessorSnapshot> processors,
        List<MemberSnapshot> members
) {

    public static final String VERSION = "v1";

    /**
     * État d'un event processor. Les champs {@code canPause/canPlay/canSplit/canMerge},
     * {@code tokenStoreIdentifier}, {@code activeThreads}, {@code capacity} et
     * {@code freeThreadInstances} reproduisent le contenu natif Axon Server.
     */
    public record ProcessorSnapshot(
            String name,
            String mode,
            boolean running,
            boolean error,
            boolean replaying,
            boolean supportsReset,
            boolean canPause,
            boolean canPlay,
            boolean canSplit,
            boolean canMerge,
            String tokenStoreIdentifier,
            int activeThreads,
            int capacity,
            List<String> freeThreadInstances,
            List<String> warnings,
            List<SegmentSnapshot> segments,
            DlqStats dlq
    ) {
    }

    public record SegmentSnapshot(
            int segmentId,
            int mask,
            boolean caughtUp,
            boolean replaying,
            boolean merging,
            boolean errorState,
            Long currentPosition,
            Long resetPosition,
            int onePartOf,
            String errorType,
            String errorMessage
    ) {
    }

    public record DlqStats(long size, long sequences) {
    }

    /**
     * Membre du bus gRPC vu par ce pod, avec ses capacités ({@code joutio.membership.MembershipService}).
     * Champ ajouté après la v1 : absent des agents plus anciens.
     */
    public record MemberSnapshot(
            String clientId,
            String componentName,
            String endpoint,
            boolean local,
            int loadFactor,
            List<String> commands,
            List<String> queries,
            Instant seenAt
    ) {
    }
}
