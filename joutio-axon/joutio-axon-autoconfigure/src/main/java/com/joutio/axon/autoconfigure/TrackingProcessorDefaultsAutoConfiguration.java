package com.joutio.axon.autoconfigure;

import org.axonframework.config.ConfigurerModule;
import org.axonframework.config.ProcessingGroup;
import org.axonframework.spring.stereotype.Saga;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.env.Environment;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Réglages communs des tracking processors ({@link TrackingProcessorProperties}).
 *
 * <p>Enregistre la configuration par défaut de tous les processors, et une configuration par nom
 * pour chaque groupe de saga non segmenté (token initial en tête, cf. {@link TrackingProcessorDefaults}).
 * Les groupes segmentés reçoivent les mêmes réglages dans leur propre autoconfiguration ; les groupes
 * instance-scopés sont des subscribing processors, sans token ni ces réglages.</p>
 */
@AutoConfiguration
@EnableConfigurationProperties({
        TrackingProcessorProperties.class,
        SegmentedProcessingGroupsProperties.class
})
public class TrackingProcessorDefaultsAutoConfiguration {

    static final String CLAIM_TIMEOUT_PROPERTY = "axon.eventhandling.tokenstore.claim-timeout";
    private static final Duration DEFAULT_CLAIM_TIMEOUT = Duration.ofSeconds(10);

    @Bean
    public TrackingProcessorDefaults trackingProcessorDefaults(TrackingProcessorProperties properties,
                                                               Environment environment,
                                                               ListableBeanFactory beanFactory) {
        Duration claimTimeout = Binder.get(environment).bind(CLAIM_TIMEOUT_PROPERTY, Duration.class)
                .orElse(DEFAULT_CLAIM_TIMEOUT);
        if (properties.eventAvailabilityTimeout().compareTo(claimTimeout) >= 0) {
            throw new IllegalStateException("joutio.axon.event-availability-timeout ("
                    + properties.eventAvailabilityTimeout() + ") must be below " + CLAIM_TIMEOUT_PROPERTY
                    + " (" + claimTimeout + "): idle workers would lose their segments.");
        }
        return new TrackingProcessorDefaults(properties.eventAvailabilityTimeout(), sagaGroups(beanFactory));
    }

    @Bean
    public ConfigurerModule trackingProcessorDefaultsConfigurer(TrackingProcessorDefaults defaults,
                                                                SegmentedProcessingGroupsProperties segmented) {
        return configurer -> configurer.eventProcessing(eventProcessing -> {
            eventProcessing.registerTrackingEventProcessorConfiguration(
                    configuration -> defaults.defaultConfiguration());
            defaults.sagaGroups().stream()
                    .filter(group -> !segmented.processors().containsKey(group))
                    .forEach(group -> eventProcessing.registerTrackingEventProcessorConfiguration(group,
                            configuration -> defaults.apply(group, defaults.defaultConfiguration())));
        });
    }

    /** Groupes des sagas Spring ({@link Saga}) ; une saga sans {@link ProcessingGroup} est refusée par ailleurs. */
    static Set<String> sagaGroups(ListableBeanFactory beanFactory) {
        return Arrays.stream(beanFactory.getBeanNamesForAnnotation(Saga.class))
                .map(beanFactory::getType)
                .filter(Objects::nonNull)
                .map(type -> AnnotatedElementUtils.findMergedAnnotation(type, ProcessingGroup.class))
                .filter(Objects::nonNull)
                .map(ProcessingGroup::value)
                .collect(Collectors.toUnmodifiableSet());
    }
}
