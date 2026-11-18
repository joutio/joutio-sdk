package com.joutio.axon.autoconfigure;

import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.commandhandling.gateway.DefaultCommandGateway;
import org.axonframework.commandhandling.gateway.ExponentialBackOffIntervalRetryScheduler;
import org.axonframework.springboot.autoconfig.AxonAutoConfiguration;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * {@link CommandGateway} du SDK : celui d'Axon, plus le nouvel essai des échecs passagers
 * ({@link TransientCommandRetryScheduler}). Une saga envoie ses commandes sans attendre : sans nouvel
 * essai, un conflit de séquence après un changement de topologie perd la commande et bloque le flux.
 */
@AutoConfiguration(before = AxonAutoConfiguration.class)
@EnableConfigurationProperties(CommandBusResilienceProperties.class)
public class CommandGatewayRetryAutoConfiguration implements DisposableBean {

    // Pas de bean ScheduledExecutorService : il désactiverait le TaskScheduler par défaut de Spring Boot.
    private final ScheduledExecutorService retryExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "axon-command-retry");
        thread.setDaemon(true);
        return thread;
    });

    @Override
    public void destroy() {
        retryExecutor.shutdownNow();
    }

    @Bean
    @ConditionalOnMissingBean
    public CommandGateway commandGateway(CommandBus commandBus,
                                         CommandBusResilienceProperties properties) {
        ExponentialBackOffIntervalRetryScheduler backOff = ExponentialBackOffIntervalRetryScheduler.builder()
                .retryExecutor(retryExecutor)
                .backoffFactor(properties.retryBackoff().toMillis())
                .maxRetryCount(properties.maxRetries())
                // Le tri transitoire / définitif est fait par TransientCommandRetryScheduler.
                .nonTransientFailurePredicate(failure -> false)
                .build();
        return DefaultCommandGateway.builder()
                .commandBus(commandBus)
                .retryScheduler(new TransientCommandRetryScheduler(backOff))
                .build();
    }
}
