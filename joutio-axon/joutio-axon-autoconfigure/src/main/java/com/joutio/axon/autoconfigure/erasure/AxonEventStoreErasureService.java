package com.joutio.axon.autoconfigure.erasure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.util.Collection;

/**
 * Effacement physique des événements d'un agrégat supprimé (RGPD).
 *
 * <p>Outil d'exploitation dédié : il n'est jamais utilisé pour réécrire un agrégat vivant. Il
 * supprime la lignée d'événements et les snapshots de l'agrégat. Les payloads sont en {@code bytea}
 * (schéma {@code V0}) : aucun <em>large object</em> à libérer. Les agrégats métier ne doivent appeler
 * cet outil qu'après la diffusion de l'événement de suppression et le traitement des projections.</p>
 */
public class AxonEventStoreErasureService {

    private static final Logger logger = LoggerFactory.getLogger(AxonEventStoreErasureService.class);

    private final JdbcTemplate jdbcTemplate;

    public AxonEventStoreErasureService(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Transactional
    public void eraseAggregates(Collection<String> aggregateIds) {
        aggregateIds.forEach(this::eraseAggregate);
    }

    @Transactional
    public void eraseAggregate(String aggregateId) {
        int events = jdbcTemplate.update(
                "DELETE FROM domain_event_entry WHERE aggregate_identifier = ?", aggregateId);
        int snapshots = jdbcTemplate.update(
                "DELETE FROM snapshot_event_entry WHERE aggregate_identifier = ?", aggregateId);
        logger.info("Event store erased for aggregate {}: {} event(s), {} snapshot(s)",
                aggregateId, events, snapshots);
    }
}
