package com.joutio.axon.autoconfigure;

import org.apache.kafka.clients.admin.NewTopic;
import org.axonframework.config.Configuration;
import org.axonframework.config.Configurer;
import org.axonframework.config.DefaultConfigurer;
import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventhandling.TrackingEventProcessor;
import org.axonframework.messaging.StreamableMessageSource;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class SegmentedProcessingGroupsAutoConfigurationTest {

    @Test
    void segmentedGroup_runsOneThreadPerSegment() {
        Configurer configurer = baseConfigurer();

        new SegmentedProcessingGroupsAutoConfiguration()
                .segmentedProcessingGroupsConfigurer(
                        new SegmentedProcessingGroupsProperties(
                                Map.of("saga-like", new SegmentedProcessingGroupsProperties.Processor(4)), null),
                        new InstanceScopedProcessingGroupsProperties(List.of(), null),
                        TrackingProcessorDefaultsTest.defaults())
                .configureModule(configurer);

        Configuration configuration = configurer.buildConfiguration();

        TrackingEventProcessor processor = configuration.eventProcessingConfiguration()
                .eventProcessor("saga-like", TrackingEventProcessor.class)
                .orElseThrow();
        assertThat(processor.maxCapacity()).isEqualTo(4);
    }

    @Test
    void unconfiguredGroup_keepsSingleThread() {
        Configurer configurer = baseConfigurer();

        new SegmentedProcessingGroupsAutoConfiguration()
                .segmentedProcessingGroupsConfigurer(
                        new SegmentedProcessingGroupsProperties(null, null),
                        new InstanceScopedProcessingGroupsProperties(List.of(), null),
                        TrackingProcessorDefaultsTest.defaults())
                .configureModule(configurer);

        Configuration configuration = configurer.buildConfiguration();

        assertThat(configuration.eventProcessingConfiguration()
                .eventProcessor("saga-like", TrackingEventProcessor.class)
                .orElseThrow()
                .maxCapacity()).isEqualTo(1);
    }

    @Test
    void instanceScopedGroup_cannotBeSegmented() {
        SegmentedProcessingGroupsAutoConfiguration autoConfiguration = new SegmentedProcessingGroupsAutoConfiguration();
        SegmentedProcessingGroupsProperties properties = new SegmentedProcessingGroupsProperties(
                Map.of("fanout", new SegmentedProcessingGroupsProperties.Processor(2)), null);
        InstanceScopedProcessingGroupsProperties instanceScoped =
                new InstanceScopedProcessingGroupsProperties(List.of("fanout"), null);

        assertThatThrownBy(() -> autoConfiguration.segmentedProcessingGroupsConfigurer(
                properties, instanceScoped, TrackingProcessorDefaultsTest.defaults()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("fanout");
    }

    @Test
    void processor_rejectsNonPositiveValues() {
        assertThatThrownBy(() -> new SegmentedProcessingGroupsProperties.Processor(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void axonTopic_hasOnePartitionByDefault() {
        NewTopic topic = new AxonDistributedKafkaAutoConfiguration().axonDefaultEventTopic(new MockEnvironment());

        assertThat(topic.numPartitions()).isEqualTo(1);
    }

    @Test
    void axonTopic_partitionsAreConfigurable() {
        MockEnvironment environment = new MockEnvironment();
        environment.setProperty(AxonDistributedKafkaAutoConfiguration.PARTITIONS_PROPERTY, "3");

        NewTopic topic = new AxonDistributedKafkaAutoConfiguration().axonDefaultEventTopic(environment);

        assertThat(topic.numPartitions()).isEqualTo(3);
    }

    @SuppressWarnings("unchecked")
    private static Configurer baseConfigurer() {
        StreamableMessageSource<TrackedEventMessage<?>> source = mock(StreamableMessageSource.class);
        Configurer configurer = DefaultConfigurer.defaultConfiguration();
        configurer.eventProcessing()
                .configureDefaultStreamableMessageSource(configuration -> source)
                .registerEventHandler(configuration -> new SagaLikeHandler());
        return configurer;
    }

    @ProcessingGroup("saga-like")
    static class SagaLikeHandler {

        @EventHandler
        public void on(SagaLikeEvent event) {
            // Handler de test : seul le rattachement au groupe compte.
        }
    }

    record SagaLikeEvent(String id) {
    }
}
