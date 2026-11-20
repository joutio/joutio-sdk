package com.joutio.axon.autoconfigure;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.axonframework.config.Configurer;
import org.axonframework.config.Configuration;
import org.axonframework.config.DefaultConfigurer;
import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.eventhandling.EventProcessor;
import org.axonframework.eventhandling.SubscribingEventProcessor;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventhandling.TrackingEventProcessor;
import org.axonframework.extensions.kafka.KafkaProperties;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.consumer.Fetcher;
import org.axonframework.messaging.StreamableMessageSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.mock.env.MockEnvironment;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class InstanceScopedProcessingGroupsAutoConfigurationTest {

    @Test
    void instanceScopedGroup_isASubscribingProcessorUnderItsOwnName() {
        Configuration configuration = configure(new InstanceScopedProcessingGroupsProperties(List.of("fanout"), "pod-1"));

        EventProcessor processor = configuration.eventProcessingConfiguration().eventProcessors().get("fanout");
        assertThat(processor).isInstanceOf(SubscribingEventProcessor.class);
    }

    @Test
    void withoutInstanceScopedGroups_theAnnotatedGroupStaysATrackingProcessor() {
        Configuration configuration = configure(new InstanceScopedProcessingGroupsProperties(List.of(), null));

        assertThat(configuration.eventProcessingConfiguration().eventProcessors().get("fanout"))
                .isInstanceOf(TrackingEventProcessor.class);
    }

    /**
     * Garde-fou anti-replay et anti-écriture : le consumer partagé démarre à la fin du topic (le défaut
     * du SDK est le début, qui ferait rejouer tout le flux à chaque pod) et ne commite aucun offset.
     */
    @Test
    void liveConsumer_startsAtTheEndOfTheTopicAndNeverCommits() {
        Map<String, Object> properties = InstanceScopedProcessingGroupsAutoConfiguration.liveConsumerProperties(Map.of(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092"));

        assertThat(properties)
                .containsEntry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
                .containsEntry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
                .containsEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
    }

    @Test
    void consumerGroup_isUniquePerServiceAndInstance_fallingBackToHostname() {
        MockEnvironment environment = new MockEnvironment()
                .withProperty("spring.application.name", "joutio-bff")
                .withProperty("HOSTNAME", "pod-42");

        assertThat(InstanceScopedProcessingGroupsAutoConfiguration.consumerGroupId(
                new InstanceScopedProcessingGroupsProperties(List.of("fanout"), null), environment))
                .isEqualTo("joutio-bff.pod-42");
        assertThat(InstanceScopedProcessingGroupsAutoConfiguration.consumerGroupId(
                new InstanceScopedProcessingGroupsProperties(List.of("fanout"), "pod-1"), environment))
                .isEqualTo("joutio-bff.pod-1");
    }

    private static Configuration configure(InstanceScopedProcessingGroupsProperties properties) {
        Configurer configurer = DefaultConfigurer.defaultConfiguration();
        configurer.eventProcessing()
                .configureDefaultStreamableMessageSource(configuration -> mockStreamableSource())
                .registerEventHandler(configuration -> new FanoutHandler());

        new InstanceScopedProcessingGroupsAutoConfiguration()
                .instanceScopedProcessingGroupsConfigurer(properties, new MockEnvironment(), applicationContext(),
                        registry())
                .configureModule(configurer);

        return configurer.buildConfiguration();
    }

    @SuppressWarnings("unchecked")
    private static StreamableMessageSource<TrackedEventMessage<?>> mockStreamableSource() {
        return mock(StreamableMessageSource.class);
    }

    private static ApplicationContext applicationContext() {
        ApplicationContext applicationContext = mock(ApplicationContext.class);
        when(applicationContext.getBean(KafkaProperties.class)).thenReturn(new KafkaProperties());
        when(applicationContext.getBean(eq("kafkaFetcher"), eq(Fetcher.class))).thenReturn(mock(Fetcher.class));
        when(applicationContext.getBean(eq("kafkaMessageConverter"), eq(KafkaMessageConverter.class)))
                .thenReturn(mock(KafkaMessageConverter.class));
        return applicationContext;
    }

    @SuppressWarnings("unchecked")
    private static ObjectProvider<io.micrometer.core.instrument.MeterRegistry> registry() {
        ObjectProvider<io.micrometer.core.instrument.MeterRegistry> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(new SimpleMeterRegistry());
        return provider;
    }

    @ProcessingGroup("fanout")
    static class FanoutHandler {

        @EventHandler
        public void on(FanoutEvent event) {
            // Handler de test : seul le rattachement au groupe compte.
        }
    }

    record FanoutEvent(String id) {
    }
}
