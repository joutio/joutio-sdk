package com.joutio.axon.autoconfigure;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import org.axonframework.config.ConfigurerModule;
import org.axonframework.eventhandling.TrackingEventProcessorConfiguration;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.context.annotation.Bean;

import java.util.Map;

/**
 * Applique {@link SegmentedProcessingGroupsProperties} aux processors tracking des groupes globaux
 * et répartit leurs segments entre répliques ({@link SegmentRebalancer}). Les réglages communs
 * ({@link TrackingProcessorDefaults}) s'appliquent aussi aux groupes segmentés.
 *
 * <p>Seule la configuration du processor change : la source reste le flux Kafka streamable posé
 * par {@link AxonDistributedKafkaAutoConfiguration} (passer par
 * {@code axon.eventhandling.processors.*} rebrancherait le groupe sur l'event store local). Un
 * groupe instance-scopé ne se segmente pas : chaque réplique y lit déjà tout le flux.</p>
 */
@AutoConfiguration
@EnableConfigurationProperties({
        SegmentedProcessingGroupsProperties.class,
        InstanceScopedProcessingGroupsProperties.class
})
public class SegmentedProcessingGroupsAutoConfiguration {

    @Bean
    public ConfigurerModule segmentedProcessingGroupsConfigurer(
            SegmentedProcessingGroupsProperties properties,
            InstanceScopedProcessingGroupsProperties instanceScoped,
            TrackingProcessorDefaults defaults) {
        Map<String, SegmentedProcessingGroupsProperties.Processor> processors = properties.processors();
        processors.keySet().stream()
                .filter(instanceScoped.instanceScopedGroups()::contains)
                .findFirst()
                .ifPresent(group -> {
                    throw new IllegalStateException(
                            "Processing group [" + group + "] is instance-scoped and cannot be segmented.");
                });
        return configurer -> configurer.eventProcessing(eventProcessing -> processors.forEach(
                (group, processor) -> eventProcessing.registerTrackingEventProcessorConfiguration(group,
                        configuration -> defaults.apply(group, TrackingEventProcessorConfiguration
                                .forParallelProcessing(processor.segments())
                                .andInitialSegmentsCount(processor.segments())))));
    }

    @Bean
    public SegmentRebalancer segmentRebalancer(
            SegmentedProcessingGroupsProperties properties,
            org.axonframework.config.Configuration axonConfiguration,
            ObjectProvider<DiscoveryClient> discoveryClient,
            ObjectProvider<MeterRegistry> registry,
            @Value("${spring.application.name:axon-distributed-application}") String serviceId) {
        return new SegmentRebalancer(properties, axonConfiguration, discoveryClient, serviceId,
                registry.getIfAvailable(() -> Metrics.globalRegistry));
    }
}
