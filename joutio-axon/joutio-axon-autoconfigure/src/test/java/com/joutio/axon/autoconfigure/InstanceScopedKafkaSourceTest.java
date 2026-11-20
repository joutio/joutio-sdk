package com.joutio.axon.autoconfigure;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.axonframework.config.Configuration;
import org.axonframework.config.Configurer;
import org.axonframework.config.DefaultConfigurer;
import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.extensions.kafka.KafkaProperties;
import org.axonframework.extensions.kafka.eventhandling.DefaultKafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.consumer.AsyncFetcher;
import org.axonframework.extensions.kafka.eventhandling.consumer.Fetcher;
import org.axonframework.messaging.StreamableMessageSource;
import org.axonframework.serialization.json.JacksonSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.mock.env.MockEnvironment;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Source des groupes instance-scopés sur un vrai broker : un seul consumer pour tous les groupes du pod,
 * démarrage à la fin du topic, aucun offset commité.
 */
@Testcontainers
class InstanceScopedKafkaSourceTest {

    private static final String TOPIC = "Axon.Events";
    private static final String GROUP_ID = "joutio-bff.pod-1";
    private static final List<String> FIRST = new CopyOnWriteArrayList<>();
    private static final List<String> SECOND = new CopyOnWriteArrayList<>();

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.7.0");

    private final KafkaMessageConverter<String, byte[]> converter = DefaultKafkaMessageConverter.builder()
            .serializer(JacksonSerializer.defaultSerializer())
            .build();
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private Configuration configuration;

    @AfterEach
    void shutdown() {
        if (configuration != null) {
            configuration.shutdown();
        }
        FIRST.clear();
        SECOND.clear();
    }

    @Test
    void everyGroupOfThePodReceivesNewEventsThroughOneConsumer_withoutReplayNorCommittedOffsets() throws Exception {
        publish("before-start");

        configuration = configure();
        configuration.start();
        awaitStableGroup();
        publish("after-start");

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(FIRST).containsExactly("after-start");
            assertThat(SECOND).containsExactly("after-start");
        });
        assertThat(registry.get("joutio.axon.kafka.consumers.open").tag("source", "instance-scoped").gauge().value())
                .isEqualTo(1.0);
        try (Admin admin = admin()) {
            assertThat(admin.listConsumerGroupOffsets(GROUP_ID).partitionsToOffsetAndMetadata().get()).isEmpty();
        }
    }

    private Configuration configure() {
        Configurer configurer = DefaultConfigurer.defaultConfiguration();
        configurer.eventProcessing()
                .configureDefaultStreamableMessageSource(configuration -> mockStreamableSource())
                .registerEventHandler(configuration -> new FirstHandler())
                .registerEventHandler(configuration -> new SecondHandler());
        new InstanceScopedProcessingGroupsAutoConfiguration()
                .instanceScopedProcessingGroupsConfigurer(
                        new InstanceScopedProcessingGroupsProperties(List.of("first", "second"), "pod-1"),
                        new MockEnvironment().withProperty("spring.application.name", "joutio-bff"),
                        applicationContext(),
                        registryProvider())
                .configureModule(configurer);
        return configurer.buildConfiguration();
    }

    private void awaitStableGroup() {
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            try (Admin admin = admin()) {
                ConsumerGroupDescription group = admin.describeConsumerGroups(List.of(GROUP_ID))
                        .describedGroups().get(GROUP_ID).get();
                return group.members().stream().anyMatch(member -> !member.assignment().topicPartitions().isEmpty());
            }
        });
        // L'assignation précède la lecture de la position de départ (fin du topic) : laisser un poll s'écouler.
        await().pollDelay(Duration.ofSeconds(2)).until(() -> true);
    }

    private void publish(String id) throws Exception {
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class))) {
            producer.send(converter.createKafkaMessage(GenericEventMessage.asEventMessage(new Ping(id)), TOPIC)).get();
        }
    }

    private static Admin admin() {
        return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()));
    }

    private ApplicationContext applicationContext() {
        KafkaProperties kafka = new KafkaProperties();
        kafka.setBootstrapServers(List.of(KAFKA.getBootstrapServers()));
        kafka.setDefaultTopic(TOPIC);
        Fetcher<?, ?, ?> fetcher = AsyncFetcher.builder().pollTimeout(200).build();
        ApplicationContext applicationContext = mock(ApplicationContext.class);
        when(applicationContext.getBean(KafkaProperties.class)).thenReturn(kafka);
        when(applicationContext.getBean(eq("kafkaFetcher"), eq(Fetcher.class))).thenReturn(fetcher);
        when(applicationContext.getBean(eq("kafkaMessageConverter"), eq(KafkaMessageConverter.class)))
                .thenReturn(converter);
        return applicationContext;
    }

    @SuppressWarnings("unchecked")
    private ObjectProvider<io.micrometer.core.instrument.MeterRegistry> registryProvider() {
        ObjectProvider<io.micrometer.core.instrument.MeterRegistry> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(registry);
        return provider;
    }

    @SuppressWarnings("unchecked")
    private static StreamableMessageSource<TrackedEventMessage<?>> mockStreamableSource() {
        return mock(StreamableMessageSource.class);
    }

    @ProcessingGroup("first")
    static class FirstHandler {

        @EventHandler
        public void on(Ping event) {
            FIRST.add(event.id());
        }
    }

    @ProcessingGroup("second")
    static class SecondHandler {

        @EventHandler
        public void on(Ping event) {
            SECOND.add(event.id());
        }
    }

    record Ping(String id) {
    }
}
