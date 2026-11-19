package com.joutio.axon.autoconfigure.erasure;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Effacement sur le vrai schéma Axon ({@code V0}, payloads {@code bytea}), dans une transaction comme
 * dans un handler : un appel SQL en échec y avorterait toute la suite.
 */
@Testcontainers
class AxonEventStoreErasureServiceTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

    private JdbcTemplate jdbc;
    private TransactionTemplate transaction;
    private AxonEventStoreErasureService erasure;

    @BeforeEach
    void schema() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration/axon")
                .cleanDisabled(false).load().clean();
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration/axon").load().migrate();
        jdbc = new JdbcTemplate(dataSource);
        transaction = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        erasure = new AxonEventStoreErasureService(dataSource);
    }

    @Test
    void erasesEventsAndSnapshotsOfTheGivenAggregatesOnly() {
        event(1, "user-1", 0);
        event(2, "user-1", 1);
        event(3, "progress:user-1", 0);
        event(4, "user-2", 0);
        snapshot("user-1", 1);

        transaction.executeWithoutResult(status -> {
            erasure.eraseAggregates(List.of("user-1", "progress:user-1"));
            // La transaction reste utilisable après l'effacement (le handler continue d'écrire).
            assertThat(jdbc.queryForObject("SELECT count(*) FROM domain_event_entry", Long.class)).isEqualTo(1L);
        });

        assertThat(jdbc.queryForList("SELECT aggregate_identifier FROM domain_event_entry", String.class))
                .containsExactly("user-2");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM snapshot_event_entry", Long.class)).isZero();
    }

    private void event(long index, String aggregateId, long sequence) {
        jdbc.update("""
                INSERT INTO domain_event_entry (global_index, event_identifier, meta_data, payload, payload_type,
                    time_stamp, aggregate_identifier, sequence_number, type)
                VALUES (?, ?, ?, ?, 'Event', '2026-11-19T00:00:00Z', ?, ?, 'Aggregate')""",
                index, "event-" + index, "{}".getBytes(), "{\"pseudonym\":\"Nadia\"}".getBytes(), aggregateId, sequence);
    }

    private void snapshot(String aggregateId, long sequence) {
        jdbc.update("""
                INSERT INTO snapshot_event_entry (aggregate_identifier, sequence_number, type, event_identifier,
                    meta_data, payload, payload_type, time_stamp)
                VALUES (?, ?, 'Aggregate', ?, ?, ?, 'Snapshot', '2026-11-19T00:00:00Z')""",
                aggregateId, sequence, "snapshot-" + aggregateId, "{}".getBytes(), "{}".getBytes());
    }
}
