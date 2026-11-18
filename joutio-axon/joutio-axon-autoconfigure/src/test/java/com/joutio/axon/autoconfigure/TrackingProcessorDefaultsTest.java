package com.joutio.axon.autoconfigure;

import org.axonframework.config.Configurer;
import org.axonframework.config.EventProcessingConfigurer;
import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.GlobalSequenceTrackingToken;
import org.axonframework.eventhandling.ReplayToken;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventhandling.TrackingEventProcessorConfiguration;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.messaging.StreamableMessageSource;
import org.axonframework.spring.stereotype.Saga;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.mock.env.MockEnvironment;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TrackingProcessorDefaultsTest {

    private static final TrackingToken HEAD = new GlobalSequenceTrackingToken(42);

    static TrackingProcessorDefaults defaults() {
        return new TrackingProcessorDefaults(Duration.ofSeconds(5), Set.of());
    }

    @Test
    void defaultConfiguration_waitsFiveSecondsBeforeExtendingTheClaim() {
        TrackingEventProcessorConfiguration configuration = defaults().defaultConfiguration();

        assertThat(configuration.getEventAvailabilityTimeout()).isEqualTo(5000);
        assertThat(configuration.getMaxThreadCount()).isEqualTo(1);
    }

    @Test
    void apply_keepsTheGroupOwnSettings() {
        TrackingEventProcessorConfiguration configuration = defaults().apply("projection",
                TrackingEventProcessorConfiguration.forParallelProcessing(4).andInitialSegmentsCount(4));

        assertThat(configuration.getEventAvailabilityTimeout()).isEqualTo(5000);
        assertThat(configuration.getMaxThreadCount()).isEqualTo(4);
        assertThat(configuration.getInitialSegmentsCount()).isEqualTo(4);
    }

    @Test
    void apply_startsSagaGroupsAtTheHeadOfTheStream() {
        TrackingProcessorDefaults defaults = new TrackingProcessorDefaults(Duration.ofSeconds(5), Set.of("a-saga"));

        TrackingToken initial = defaults.apply("a-saga", defaults.defaultConfiguration())
                .getInitialTrackingToken().apply(sourceAt(HEAD));

        assertThat(initial).isEqualTo(HEAD);
    }

    @Test
    void apply_leavesOtherGroupsOnAxonInitialToken() {
        TrackingProcessorDefaults defaults = new TrackingProcessorDefaults(Duration.ofSeconds(5), Set.of("a-saga"));

        TrackingToken initial = defaults.apply("projection", defaults.defaultConfiguration())
                .getInitialTrackingToken().apply(sourceAt(HEAD));

        assertThat(initial).isInstanceOf(ReplayToken.class);
    }

    @Test
    void properties_defaultToFiveSecondsAndRejectNonPositive() {
        assertThat(new TrackingProcessorProperties(null).eventAvailabilityTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThatThrownBy(() -> new TrackingProcessorProperties(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void timeout_mustStayBelowTheClaimTimeout() {
        MockEnvironment environment = new MockEnvironment();
        environment.setProperty(TrackingProcessorDefaultsAutoConfiguration.CLAIM_TIMEOUT_PROPERTY, "5s");

        try (GenericApplicationContext context = new GenericApplicationContext()) {
            context.refresh();
            assertThatThrownBy(() -> new TrackingProcessorDefaultsAutoConfiguration().trackingProcessorDefaults(
                    new TrackingProcessorProperties(Duration.ofSeconds(5)), environment, context))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("claim-timeout");
        }
    }

    @Test
    void sagaGroups_areReadFromSpringSagas() {
        try (GenericApplicationContext context = new GenericApplicationContext()) {
            context.registerBean(SampleSaga.class);
            context.registerBean(SampleProjection.class);

            assertThat(TrackingProcessorDefaultsAutoConfiguration.sagaGroups(context.getBeanFactory()))
                    .containsExactly("sample-saga");
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void configurer_registersTheDefaultAndNonSegmentedSagaGroups() {
        TrackingProcessorDefaults defaults =
                new TrackingProcessorDefaults(Duration.ofSeconds(5), Set.of("plain-saga", "segmented-saga"));
        SegmentedProcessingGroupsProperties segmented = new SegmentedProcessingGroupsProperties(
                Map.of("segmented-saga", new SegmentedProcessingGroupsProperties.Processor(2)), null);
        EventProcessingConfigurer eventProcessing = mock(EventProcessingConfigurer.class, RETURNS_SELF);
        Configurer configurer = mock(Configurer.class);
        when(configurer.eventProcessing(any(Consumer.class))).thenAnswer(invocation -> {
            invocation.<Consumer<EventProcessingConfigurer>>getArgument(0).accept(eventProcessing);
            return configurer;
        });

        new TrackingProcessorDefaultsAutoConfiguration()
                .trackingProcessorDefaultsConfigurer(defaults, segmented)
                .configureModule(configurer);

        ArgumentCaptor<Function<org.axonframework.config.Configuration, TrackingEventProcessorConfiguration>> captor =
                ArgumentCaptor.forClass(Function.class);
        verify(eventProcessing).registerTrackingEventProcessorConfiguration(captor.capture());
        assertThat(captor.getValue().apply(null).getEventAvailabilityTimeout()).isEqualTo(5000);
        verify(eventProcessing).registerTrackingEventProcessorConfiguration(eq("plain-saga"), any(Function.class));
        verify(eventProcessing, never())
                .registerTrackingEventProcessorConfiguration(eq("segmented-saga"), any(Function.class));
        verify(eventProcessing, never()).registerTrackingEventProcessorConfiguration(
                eq("sample-projection"), any(Function.class));
    }

    @SuppressWarnings("unchecked")
    private static StreamableMessageSource<TrackedEventMessage<?>> sourceAt(TrackingToken head) {
        StreamableMessageSource<TrackedEventMessage<?>> source = mock(StreamableMessageSource.class);
        when(source.createHeadToken()).thenReturn(head);
        return source;
    }

    @Saga
    @ProcessingGroup("sample-saga")
    static class SampleSaga {
    }

    @ProcessingGroup("sample-projection")
    static class SampleProjection {
    }
}
