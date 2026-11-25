package com.joutio.axon.autoconfigure;

import org.axonframework.extensions.springcloud.commandhandling.mode.CapabilityDiscoveryMode;
import org.axonframework.extensions.springcloud.commandhandling.mode.RestCapabilityDiscoveryMode;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * Découverte des capacités du bus de commandes : {@link LastKnownCapabilitiesDiscoveryMode} autour du
 * mode REST d'Axon, à la place de son ignore-listing (désactivé par
 * {@link AxonDistributedEnvironmentPostProcessor}).
 */
// Sans `after = SpringCloudAutoConfiguration` : cet ordre ferait passer le RestTemplate sans jeton
// d'Axon avant celui du SDK. Le @Primary suffit à l'injection dans le SpringCloudCommandRouter.
@AutoConfiguration
@ConditionalOnProperty("axon.distributed.enabled")
@EnableConfigurationProperties(CommandBusResilienceProperties.class)
public class CapabilityDiscoveryAutoConfiguration {

    @Bean
    @Primary
    @ConditionalOnProperty(value = "axon.distributed.spring-cloud.mode", havingValue = "rest", matchIfMissing = true)
    public CapabilityDiscoveryMode lastKnownCapabilityDiscoveryMode(RestCapabilityDiscoveryMode restCapabilityDiscoveryMode,
                                                                    CommandBusResilienceProperties properties,
                                                                    LocalCommandCapabilities localCommandCapabilities) {
        return new LastKnownCapabilitiesDiscoveryMode(restCapabilityDiscoveryMode, properties.capabilitiesRetention(),
                localCommandCapabilities);
    }

    @Bean
    public LocalCommandCapabilities localCommandCapabilities() {
        return new LocalCommandCapabilities();
    }
}
