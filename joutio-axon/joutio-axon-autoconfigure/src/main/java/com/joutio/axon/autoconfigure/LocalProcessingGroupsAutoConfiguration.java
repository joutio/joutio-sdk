package com.joutio.axon.autoconfigure;

import org.axonframework.config.Configuration;
import org.axonframework.config.ConfigurerModule;
import org.axonframework.eventhandling.LoggingErrorHandler;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.List;

/**
 * Processing groups <b>locaux</b> (cf. {@link LocalProcessingGroupsProperties}) : un <b>subscribing processor</b> par
 * groupe, abonné à l'event bus du pod (l'event store embarqué), et non à Kafka.
 *
 * <p>Ses handlers s'exécutent dans l'unité de travail qui publie l'événement (commande, saga, deadline), sur le pod
 * qui l'a publié : aucun rejeu, aucun token, et un pod ne voit que ses propres événements. Ils doivent rester brefs et
 * sans I/O. Une exception y est journalisée et n'annule jamais la commande ({@link LoggingErrorHandler}).</p>
 */
@AutoConfiguration
@EnableConfigurationProperties(LocalProcessingGroupsProperties.class)
public class LocalProcessingGroupsAutoConfiguration {

    @Bean
    public ConfigurerModule localProcessingGroupsConfigurer(LocalProcessingGroupsProperties properties) {
        List<String> groups = properties.localGroups();
        return configurer -> configurer.eventProcessing(eventProcessing -> groups.forEach(group -> eventProcessing
                .registerSubscribingEventProcessor(group, Configuration::eventBus)
                .registerListenerInvocationErrorHandler(group, configuration -> new LoggingErrorHandler())));
    }
}
