package com.joutio.axon.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Map;

/**
 * Processing groups <b>segmentés</b> (`joutio.axon.processors.<groupe>.segments`).
 *
 * <p>Le parallélisme d'un groupe global vient des <b>segments Axon</b>, pas des partitions Kafka :
 * la source streamable Kafka fait lire tout le topic à chaque segment, qui ne traite que les
 * séquences (identifiant d'agrégat, identifiant de saga) qui lui reviennent. L'ordre par agrégat est
 * donc conservé quel que soit le nombre de segments.</p>
 *
 * <p>{@code segments} : nombre initial de segments, appliqué à la création du token du groupe
 * (base vierge) ; ensuite, split/merge par la console. Chaque réplique a autant de threads que de
 * segments : seule, elle les prend tous. {@link SegmentRebalancer} répartit les segments entre les
 * répliques découvertes, toutes les {@code rebalanceInterval} (défaut 30 s).</p>
 */
@ConfigurationProperties(prefix = "joutio.axon")
public record SegmentedProcessingGroupsProperties(Map<String, Processor> processors, Duration rebalanceInterval) {

    private static final Duration DEFAULT_REBALANCE_INTERVAL = Duration.ofSeconds(30);

    public SegmentedProcessingGroupsProperties {
        processors = processors == null ? Map.of() : Map.copyOf(processors);
        rebalanceInterval = rebalanceInterval == null ? DEFAULT_REBALANCE_INTERVAL : rebalanceInterval;
    }

    public record Processor(int segments) {

        public Processor {
            if (segments < 1) {
                throw new IllegalArgumentException("joutio.axon.processors.*.segments must be >= 1");
            }
        }
    }
}
