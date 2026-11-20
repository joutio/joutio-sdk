package com.joutio.axon.autoconfigure;

import org.axonframework.config.Configuration;
import org.axonframework.config.EventProcessingConfiguration;
import org.axonframework.eventhandling.EventTrackerStatus;
import org.axonframework.eventhandling.StreamingEventProcessor;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.health.Status;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class InstanceScopedProcessorsHealthIndicatorTest {

    private static final String GROUP = "game-notification-pod-a";

    @Test
    void withoutInstanceScopedGroups_isUp() {
        var indicator = new InstanceScopedProcessorsHealthIndicator(provider(null), List.of());

        assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void outOfServiceUntilEveryGroupCaughtUp_thenStaysUp() {
        EventTrackerStatus status = mock(EventTrackerStatus.class);
        StreamingEventProcessor processor = mock(StreamingEventProcessor.class);
        when(processor.isRunning()).thenReturn(true);
        when(processor.processingStatus()).thenReturn(Map.of(0, status));
        var indicator = new InstanceScopedProcessorsHealthIndicator(provider(processor), List.of(GROUP));

        when(status.isCaughtUp()).thenReturn(false);
        assertThat(indicator.health().getStatus()).isEqualTo(Status.OUT_OF_SERVICE);
        assertThat(indicator.health().getDetails()).containsEntry("lagging", List.of(GROUP));

        when(status.isCaughtUp()).thenReturn(true);
        assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);

        // Un retard passager sous charge ne retire pas le pod du service.
        when(status.isCaughtUp()).thenReturn(false);
        assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void processorWithoutClaimedSegment_isNotReady() {
        StreamingEventProcessor processor = mock(StreamingEventProcessor.class);
        when(processor.isRunning()).thenReturn(true);
        when(processor.processingStatus()).thenReturn(Map.of());
        var indicator = new InstanceScopedProcessorsHealthIndicator(provider(processor), List.of(GROUP));

        assertThat(indicator.health().getStatus()).isEqualTo(Status.OUT_OF_SERVICE);
    }

    @SuppressWarnings("unchecked")
    private static ObjectProvider<Configuration> provider(StreamingEventProcessor processor) {
        ObjectProvider<Configuration> provider = mock(ObjectProvider.class);
        if (processor != null) {
            EventProcessingConfiguration eventProcessing = mock(EventProcessingConfiguration.class);
            when(eventProcessing.eventProcessor(GROUP, StreamingEventProcessor.class)).thenReturn(Optional.of(processor));
            Configuration configuration = mock(Configuration.class);
            when(configuration.eventProcessingConfiguration()).thenReturn(eventProcessing);
            when(provider.getIfAvailable()).thenReturn(configuration);
        }
        return provider;
    }
}
