package com.joutio.axon.autoconfigure;

import com.joutio.axon.autoconfigure.metrics.MeteredConsumerFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.axonframework.config.ConfigurerModule;
import org.axonframework.extensions.kafka.KafkaProperties;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.DefaultConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.Fetcher;
import org.axonframework.extensions.kafka.eventhandling.consumer.subscribable.SubscribableKafkaMessageSource;
import org.axonframework.lifecycle.Phase;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Processing groups <b>par instance</b> : chaque réplique d'un service à fan-out temps réel reçoit
 * tous les événements (cf. {@link InstanceScopedProcessingGroupsProperties}).
 *
 * <p>Chaque groupe listé est un <b>subscribing processor</b>, le type Axon du traitement en direct
 * sans rejeu. Tous les groupes d'une instance partagent une seule
 * {@link SubscribableKafkaMessageSource} : un consumer Kafka par pod, dans un consumer group propre à
 * l'instance ({@code <application>.<instanceId>}), qui démarre à la fin du topic et ne commite aucun
 * offset. Aucune position n'est donc écrite, ni dans {@code token_entry} ni dans Kafka : un pod qui
 * démarre ne reçoit que les événements à venir, les clients complètent l'historique par les folds
 * REST.</p>
 *
 * <p>Les handlers de ces groupes s'exécutent sur le thread du consumer, l'un après l'autre : ils
 * restent non bloquants (envoi STOMP, appels asynchrones), sinon un handler lent retarde tous les
 * groupes du pod. Une exception est journalisée et l'événement suivant est traité (pas de DLQ).</p>
 */
@AutoConfiguration
@EnableConfigurationProperties(InstanceScopedProcessingGroupsProperties.class)
public class InstanceScopedProcessingGroupsAutoConfiguration {

    private static final String FALLBACK_INSTANCE_ID = "local";
    private static final String FETCHER_BEAN = "kafkaFetcher";
    private static final String MESSAGE_CONVERTER_BEAN = "kafkaMessageConverter";
    /** Démarre la source après l'abonnement de tous les processors (phase {@code INBOUND_EVENT_CONNECTORS}). */
    static final int SOURCE_PHASE = Phase.INBOUND_EVENT_CONNECTORS + 1;

    @Bean
    public ConfigurerModule instanceScopedProcessingGroupsConfigurer(
            InstanceScopedProcessingGroupsProperties properties,
            Environment environment,
            ApplicationContext applicationContext,
            ObjectProvider<MeterRegistry> registry) {
        List<String> groups = properties.instanceScopedGroups();
        if (groups.isEmpty()) {
            return configurer -> {
                // Aucun groupe instance-scopé : configuration inchangée.
            };
        }
        String groupId = consumerGroupId(properties, environment);
        return configurer -> {
            Supplier<SubscribableKafkaMessageSource<String, byte[]>> source =
                    memoize(() -> liveSource(applicationContext, registry.getIfAvailable(), groupId));
            configurer.eventProcessing(eventProcessing -> groups.forEach(group ->
                    eventProcessing.registerSubscribingEventProcessor(group, configuration -> source.get())));
            configurer.onStart(SOURCE_PHASE, () -> source.get().start());
            configurer.onShutdown(SOURCE_PHASE, () -> source.get().close());
        };
    }

    /**
     * Propriétés du consumer partagé : celles de {@code axon.kafka.consumer}, sauf le point de départ
     * (fin du topic, le défaut du SDK étant le début) et le commit d'offsets (aucun).
     */
    static Map<String, Object> liveConsumerProperties(Map<String, Object> base) {
        Map<String, Object> properties = new HashMap<>(base);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return properties;
    }

    @SuppressWarnings("unchecked")
    private static SubscribableKafkaMessageSource<String, byte[]> liveSource(
            ApplicationContext applicationContext, MeterRegistry registry, String groupId) {
        KafkaProperties kafka = applicationContext.getBean(KafkaProperties.class);
        ConsumerFactory<String, byte[]> consumers =
                new DefaultConsumerFactory<>(liveConsumerProperties(kafka.buildConsumerProperties()));
        if (registry != null) {
            consumers = new MeteredConsumerFactory<>(consumers, registry, Tags.of("source", "instance-scoped"));
        }
        return SubscribableKafkaMessageSource.<String, byte[]>builder()
                .topics(List.of(kafka.getDefaultTopic()))
                .groupId(groupId)
                .consumerFactory(consumers)
                .fetcher(applicationContext.getBean(FETCHER_BEAN, Fetcher.class))
                .messageConverter(applicationContext.getBean(MESSAGE_CONVERTER_BEAN, KafkaMessageConverter.class))
                .consumerCount(1)
                .build();
    }

    private static <T> Supplier<T> memoize(Supplier<T> supplier) {
        return new Supplier<>() {
            private T value;

            @Override
            public synchronized T get() {
                if (value == null) {
                    value = supplier.get();
                }
                return value;
            }
        };
    }

    /** Consumer group Kafka de la source : {@code <application>.<instanceId>}, unique par pod et par service. */
    static String consumerGroupId(InstanceScopedProcessingGroupsProperties properties, Environment environment) {
        return environment.getProperty("spring.application.name", "joutio") + "." + instanceId(properties, environment);
    }

    private static String instanceId(InstanceScopedProcessingGroupsProperties properties, Environment environment) {
        String configured = properties.instanceId();
        if (configured != null && !configured.isBlank()) {
            return configured.trim();
        }
        return environment.getProperty("HOSTNAME", FALLBACK_INSTANCE_ID);
    }
}
