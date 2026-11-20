package com.joutio.axon.autoconfigure.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class MeteredConsumerFactoryTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final MeteredConsumerFactory<String, byte[]> factory = new MeteredConsumerFactory<>(
            groupId -> new MockConsumer<>(OffsetResetStrategy.EARLIEST), registry);

    @Test
    void tracksOpenAndOpenedConsumers() {
        Consumer<String, byte[]> first = factory.createConsumer(null);
        Consumer<String, byte[]> second = factory.createConsumer(null);

        assertThat(open()).isEqualTo(2.0);
        assertThat(opened()).isEqualTo(2.0);

        first.close();
        second.close(Duration.ofSeconds(1));

        assertThat(open()).isZero();
        assertThat(opened()).isEqualTo(2.0);
    }

    @Test
    void closingTwiceCountsOnce() {
        Consumer<String, byte[]> consumer = factory.createConsumer(null);

        consumer.close();
        consumer.close();

        assertThat(open()).isZero();
    }

    @Test
    void delegatesToTheKafkaConsumer() {
        Consumer<String, byte[]> consumer = factory.createConsumer(null);

        assertThat(consumer.subscription()).isEmpty();
        assertThat(consumer.metrics()).isNotNull();
    }

    private double open() {
        return registry.get("joutio.axon.kafka.consumers.open").gauge().value();
    }

    private double opened() {
        return registry.get("joutio.axon.kafka.consumers.opened").counter().count();
    }
}
