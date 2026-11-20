package com.joutio.axon.autoconfigure.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import org.apache.kafka.clients.consumer.Consumer;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mesure les consumers Kafka ouverts par l'extension Axon Kafka.
 *
 * <p>La source streamable ouvre un consumer par segment de chaque processor, et un consumer
 * éphémère à chaque calcul de token de tête ou de queue. Chaque consumer est lié à
 * {@link KafkaClientMetrics} ({@code kafka_consumer_*}, tag {@code client_id}) jusqu'à sa
 * fermeture, et deux compteurs suivent leur cycle de vie :</p>
 * <ul>
 *     <li>{@code joutio.axon.kafka.consumers.open} : consumers ouverts ;</li>
 *     <li>{@code joutio.axon.kafka.consumers.created} : consumers créés depuis le démarrage (une
 *     pente au repos signale des ouvertures en boucle).</li>
 * </ul>
 */
public class MeteredConsumerFactory<K, V> implements ConsumerFactory<K, V> {

    private final ConsumerFactory<K, V> delegate;
    private final MeterRegistry registry;
    private final AtomicInteger open = new AtomicInteger();
    private final Counter created;

    public MeteredConsumerFactory(ConsumerFactory<K, V> delegate, MeterRegistry registry) {
        this.delegate = delegate;
        this.registry = registry;
        Gauge.builder("joutio.axon.kafka.consumers.open", open, AtomicInteger::get)
                .description("Consumers Kafka ouverts par la source Axon")
                .register(registry);
        this.created = Counter.builder("joutio.axon.kafka.consumers.created")
                .description("Consumers Kafka créés par la source Axon")
                .register(registry);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Consumer<K, V> createConsumer(String groupId) {
        Consumer<K, V> consumer = delegate.createConsumer(groupId);
        KafkaClientMetrics metrics = new KafkaClientMetrics(consumer);
        metrics.bindTo(registry);
        created.increment();
        open.incrementAndGet();
        AtomicBoolean closed = new AtomicBoolean();
        return (Consumer<K, V>) Proxy.newProxyInstance(
                Consumer.class.getClassLoader(),
                new Class<?>[]{Consumer.class},
                (proxy, method, args) -> {
                    if ("close".equals(method.getName()) && closed.compareAndSet(false, true)) {
                        metrics.close();
                        open.decrementAndGet();
                    }
                    try {
                        return method.invoke(consumer, args);
                    } catch (InvocationTargetException exception) {
                        throw exception.getCause();
                    }
                });
    }
}
