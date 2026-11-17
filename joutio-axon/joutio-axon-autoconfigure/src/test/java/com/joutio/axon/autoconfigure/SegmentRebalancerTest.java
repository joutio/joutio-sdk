package com.joutio.axon.autoconfigure;

import org.axonframework.common.transaction.NoTransactionManager;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.config.Configuration;
import org.axonframework.config.EventProcessingConfiguration;
import org.axonframework.eventhandling.EventTrackerStatus;
import org.axonframework.eventhandling.Segment;
import org.axonframework.eventhandling.TrackingEventProcessor;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SegmentRebalancerTest {

    private static final String GROUP = "room-saga";

    @Test
    void share_isCeilOfSegmentsPerInstance() {
        assertThat(SegmentRebalancer.segmentsToRelease(4, 4, 2)).isEqualTo(2);
        assertThat(SegmentRebalancer.segmentsToRelease(2, 4, 2)).isZero();
        assertThat(SegmentRebalancer.segmentsToRelease(4, 4, 3)).isEqualTo(2);
        assertThat(SegmentRebalancer.segmentsToRelease(1, 4, 3)).isZero();
    }

    @Test
    void singleInstance_keepsAllSegments() {
        assertThat(SegmentRebalancer.segmentsToRelease(4, 4, 1)).isZero();
        assertThat(SegmentRebalancer.segmentsToRelease(4, 4, 0)).isZero();
    }

    @Test
    void rebalance_releasesExcessSegmentsForTwoReleaseIntervals() {
        TrackingEventProcessor processor = runningProcessorOwning(0, 1, 2, 3);
        SegmentRebalancer rebalancer = rebalancer(processor, 4, 2);

        rebalancer.rebalance();

        verify(processor, times(2)).releaseSegment(anyInt(), eq(60_000L), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void rebalance_keepsFairShare() {
        TrackingEventProcessor processor = runningProcessorOwning(0, 1);
        SegmentRebalancer rebalancer = rebalancer(processor, 4, 2);

        rebalancer.rebalance();

        verify(processor, never()).releaseSegment(anyInt(), anyLong(), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void rebalance_suspendsWhenReleasedSegmentsAreNotClaimedByAnotherReplica() {
        TrackingEventProcessor processor = runningProcessorOwning(0, 1, 2, 3);
        MutableClock clock = new MutableClock();
        TokenStore tokenStore = tokenStore(4);
        SegmentRebalancer rebalancer = rebalancer(processor, tokenStore, 2, clock);

        rebalancer.rebalance();
        verify(processor, times(2)).releaseSegment(anyInt(), anyLong(), eq(TimeUnit.MILLISECONDS));

        // Personne n'a repris les segments libérés : ils restent disponibles.
        when(tokenStore.fetchAvailableSegments(GROUP)).thenReturn(List.of(Segment.computeSegment(0, 0, 1, 2, 3)));
        clock.advance(Duration.ofSeconds(30));
        rebalancer.rebalance();
        clock.advance(Duration.ofSeconds(30));
        rebalancer.rebalance();

        verify(processor, times(2)).releaseSegment(anyInt(), anyLong(), eq(TimeUnit.MILLISECONDS));

        // Fin de suspension : le rééquilibrage reprend.
        clock.advance(Duration.ofSeconds(30L * SegmentRebalancer.SUSPENSION_INTERVALS));
        rebalancer.rebalance();

        verify(processor, times(4)).releaseSegment(anyInt(), anyLong(), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void rebalance_keepsGoingWhenReleasedSegmentsWereClaimed() {
        TrackingEventProcessor processor = runningProcessorOwning(0, 1, 2, 3);
        MutableClock clock = new MutableClock();
        TokenStore tokenStore = tokenStore(4);
        SegmentRebalancer rebalancer = rebalancer(processor, tokenStore, 2, clock);

        rebalancer.rebalance();
        clock.advance(Duration.ofSeconds(30));
        rebalancer.rebalance();

        verify(processor, times(4)).releaseSegment(anyInt(), anyLong(), eq(TimeUnit.MILLISECONDS));
    }

    private static TrackingEventProcessor runningProcessorOwning(Integer... segments) {
        TrackingEventProcessor processor = mock(TrackingEventProcessor.class);
        when(processor.isRunning()).thenReturn(true);
        Map<Integer, EventTrackerStatus> status = new java.util.HashMap<>();
        for (Integer segment : segments) {
            status.put(segment, mock(EventTrackerStatus.class));
        }
        when(processor.processingStatus()).thenReturn(Collections.unmodifiableMap(status));
        return processor;
    }

    private static TokenStore tokenStore(int totalSegments) {
        TokenStore tokenStore = mock(TokenStore.class);
        when(tokenStore.fetchSegments(GROUP)).thenReturn(new int[totalSegments]);
        when(tokenStore.fetchAvailableSegments(GROUP)).thenReturn(List.of());
        return tokenStore;
    }

    private static SegmentRebalancer rebalancer(TrackingEventProcessor processor, int totalSegments, int instances) {
        return rebalancer(processor, tokenStore(totalSegments), instances, new MutableClock());
    }

    @SuppressWarnings("unchecked")
    private static SegmentRebalancer rebalancer(TrackingEventProcessor processor, TokenStore tokenStore,
                                                int instances, Clock clock) {
        int totalSegments = tokenStore.fetchSegments(GROUP).length;
        EventProcessingConfiguration eventProcessing = mock(EventProcessingConfiguration.class);
        when(eventProcessing.eventProcessor(GROUP, TrackingEventProcessor.class)).thenReturn(Optional.of(processor));
        when(eventProcessing.tokenStore(GROUP)).thenReturn(tokenStore);
        Configuration configuration = mock(Configuration.class);
        when(configuration.eventProcessingConfiguration()).thenReturn(eventProcessing);
        when(configuration.getComponent(eq(TransactionManager.class), any())).thenReturn(NoTransactionManager.INSTANCE);

        DiscoveryClient discoveryClient = mock(DiscoveryClient.class);
        List<ServiceInstance> serviceInstances = java.util.stream.Stream
                .generate(() -> mock(ServiceInstance.class)).limit(instances).toList();
        when(discoveryClient.getInstances("joutio-matchmaking")).thenReturn(serviceInstances);
        ObjectProvider<DiscoveryClient> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(discoveryClient);

        SegmentedProcessingGroupsProperties properties = new SegmentedProcessingGroupsProperties(
                Map.of(GROUP, new SegmentedProcessingGroupsProperties.Processor(totalSegments)),
                Duration.ofSeconds(30));
        return new SegmentRebalancer(properties, configuration, provider, "joutio-matchmaking", clock);
    }

    private static final class MutableClock extends Clock {

        private Instant now = Instant.parse("2026-01-01T00:00:00Z");

        void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
