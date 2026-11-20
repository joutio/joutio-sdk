package com.joutio.axon.autoconfigure;

import com.joutio.axon.autoconfigure.metrics.MeteredConsumerFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class AxonDistributedActivityMetricsAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
            .withBean("kafkaConsumerFactory", ConsumerFactory.class,
                    () -> groupId -> new MockConsumer<>(OffsetResetStrategy.EARLIEST))
            .withConfiguration(AutoConfigurations.of(AxonDistributedActivityMetricsAutoConfiguration.class));

    @Test
    void decoratesTheAxonKafkaConsumerFactory() {
        runner.run(context -> {
            assertThat(context.getBean("kafkaConsumerFactory")).isInstanceOf(MeteredConsumerFactory.class);
            context.getBean(ConsumerFactory.class).createConsumer(null);
            assertThat(context.getBean(MeterRegistry.class).get("joutio.axon.kafka.consumers.open").gauge().value())
                    .isEqualTo(1.0);
        });
    }
}
