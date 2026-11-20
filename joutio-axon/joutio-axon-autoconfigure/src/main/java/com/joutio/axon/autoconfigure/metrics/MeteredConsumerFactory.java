package com.joutio.axon.autoconfigure.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
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
 * {@link KafkaClientMetrics} ({@code kafka_consumer_*}) jusqu'à sa fermeture, avec le tag
 * {@code consumer} (rang de création) : tous les consumers d'un service partagent le même
 * {@code client_id}, et leurs séries se recouvriraient sans lui. Deux métriques suivent leur cycle
 * de vie :</p>
 * <ul>
 *     <li>{@code joutio.axon.kafka.consumers.open} : consumers ouverts ;</li>
 *     <li>{@code joutio.axon.kafka.consumers.opened} : consumers ouverts depuis le démarrage (une
 *     pente au repos signale des ouvertures en boucle). Pas de suffixe {@code .created} : réservé
 *     par OpenMetrics, l'export Prometheus le retire.</li>
 * </ul>
 *
 * <p>Chaque binder garde son propre ordonnanceur : {@link KafkaClientMetrics#close()} n'annule pas
 * sa tâche sur un ordonnanceur partagé.</p>
 */
public class MeteredConsumerFactory<K, V> implements ConsumerFactory<K, V> {

    private final ConsumerFactory<K, V> delegate;
    private final MeterRegistry registry;
    private final AtomicInteger open = new AtomicInteger();
    private final AtomicInteger sequence = new AtomicInteger();
    private final Counter opened;

    public MeteredConsumerFactory(ConsumerFactory<K, V> delegate, MeterRegistry registry) {
        this.delegate = delegate;
        this.registry = registry;
        Gauge.builder("joutio.axon.kafka.consumers.open", open, AtomicInteger::get)
                .description("Consumers Kafka ouverts par la source Axon")
                .register(registry);
        this.opened = Counter.builder("joutio.axon.kafka.consumers.opened")
                .description("Consumers Kafka ouverts par la source Axon depuis le démarrage")
                .register(registry);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Consumer<K, V> createConsumer(String groupId) {
        Consumer<K, V> consumer = delegate.createConsumer(groupId);
        KafkaClientMetrics metrics = new KafkaClientMetrics(consumer,
                Tags.of("consumer", String.valueOf(sequence.incrementAndGet())));
        metrics.bindTo(registry);
        opened.increment();
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
