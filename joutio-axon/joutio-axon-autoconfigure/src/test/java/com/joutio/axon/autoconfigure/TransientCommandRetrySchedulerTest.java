package com.joutio.axon.autoconfigure;

import org.axonframework.commandhandling.CommandExecutionException;
import org.axonframework.commandhandling.SimpleCommandBus;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.RemoteExceptionDescription;
import org.axonframework.messaging.RemoteHandlingException;
import org.axonframework.modelling.command.ConcurrencyException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransientCommandRetrySchedulerTest {

    @Test
    void localSequenceConflict_isTransient() {
        assertThat(TransientCommandRetryScheduler.isTransient(new ConcurrencyException("seq 5"))).isTrue();
    }

    @Test
    void remoteSequenceConflict_isTransient() {
        RemoteHandlingException remote = new RemoteHandlingException(new RemoteExceptionDescription(List.of(
                "org.axonframework.modelling.command.ConcurrencyException: An event for aggregate [g] at sequence [5]")));

        assertThat(TransientCommandRetryScheduler.isTransient(
                new CommandExecutionException("The remote handler threw an exception", remote))).isTrue();
    }

    @Test
    void businessRefusal_isNotRetried() {
        RemoteHandlingException remote = new RemoteHandlingException(new RemoteExceptionDescription(List.of(
                "com.joutio.game.domain.exception.GameProblems$GameAlreadyEndedProblem: ended")));

        assertThat(TransientCommandRetryScheduler.isTransient(
                new CommandExecutionException("The remote handler threw an exception", remote))).isFalse();
        assertThat(TransientCommandRetryScheduler.isTransient(new IllegalStateException("refus"))).isFalse();
    }

    @Test
    void gateway_retriesASequenceConflictThenSucceeds() throws Exception {
        SimpleCommandBus commandBus = SimpleCommandBus.builder().build();
        AtomicInteger attempts = new AtomicInteger();
        commandBus.subscribe(String.class.getName(), command -> {
            if (attempts.incrementAndGet() == 1) {
                throw new ConcurrencyException("agrégat en cache périmé");
            }
            return "ok";
        });
        CommandGatewayRetryAutoConfiguration autoConfiguration = new CommandGatewayRetryAutoConfiguration();
        try {
            CommandGateway gateway = autoConfiguration.commandGateway(commandBus,
                    new CommandBusResilienceProperties(3, Duration.ofMillis(10), null));

            Object result = gateway.send("close-round").get();

            assertThat(result).isEqualTo("ok");
            assertThat(attempts).hasValue(2);
        } finally {
            autoConfiguration.destroy();
        }
    }

    @Test
    void gateway_doesNotRetryABusinessRefusal() {
        SimpleCommandBus commandBus = SimpleCommandBus.builder().build();
        AtomicInteger attempts = new AtomicInteger();
        commandBus.subscribe(String.class.getName(), command -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("partie terminée");
        });
        CommandGatewayRetryAutoConfiguration autoConfiguration = new CommandGatewayRetryAutoConfiguration();
        try {
            CommandGateway gateway = autoConfiguration.commandGateway(commandBus,
                    new CommandBusResilienceProperties(3, Duration.ofMillis(10), null));

            assertThatThrownBy(() -> gateway.send("close-round").get()).hasCauseInstanceOf(IllegalStateException.class);
            assertThat(attempts).hasValue(1);
        } finally {
            autoConfiguration.destroy();
        }
    }
}
