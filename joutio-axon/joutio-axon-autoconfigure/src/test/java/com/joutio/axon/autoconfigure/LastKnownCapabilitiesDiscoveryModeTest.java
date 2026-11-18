package com.joutio.axon.autoconfigure;

import org.axonframework.commandhandling.distributed.commandfilter.AcceptAll;
import org.axonframework.extensions.springcloud.commandhandling.mode.CapabilityDiscoveryMode;
import org.axonframework.extensions.springcloud.commandhandling.mode.DefaultMemberCapabilities;
import org.axonframework.extensions.springcloud.commandhandling.mode.MemberCapabilities;
import org.axonframework.extensions.springcloud.commandhandling.mode.ServiceInstanceClientException;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LastKnownCapabilitiesDiscoveryModeTest {

    private static final ServiceInstance GAME_B =
            new DefaultServiceInstance("game-b", "joutio-game", "10.42.0.2", 8080, false);
    private static final MemberCapabilities CAPABLE = new DefaultMemberCapabilities(100, AcceptAll.INSTANCE);

    private final CapabilityDiscoveryMode delegate = mock(CapabilityDiscoveryMode.class);
    private final MutableClock clock = new MutableClock();
    private final LastKnownCapabilitiesDiscoveryMode mode =
            new LastKnownCapabilitiesDiscoveryMode(delegate, Duration.ofMinutes(2), clock);

    @Test
    void clientError_keepsLastKnownCapabilities() {
        when(delegate.capabilities(GAME_B)).thenReturn(Optional.of(CAPABLE))
                .thenThrow(new ServiceInstanceClientException("401", null));

        mode.capabilities(GAME_B);
        clock.advance(Duration.ofSeconds(30));

        assertThat(mode.capabilities(GAME_B)).contains(CAPABLE);
    }

    @Test
    void unreachableInstance_keepsLastKnownCapabilities() {
        when(delegate.capabilities(GAME_B)).thenReturn(Optional.of(CAPABLE))
                .thenReturn(Optional.of(DefaultMemberCapabilities.INCAPABLE_MEMBER));

        mode.capabilities(GAME_B);

        assertThat(mode.capabilities(GAME_B)).contains(CAPABLE);
    }

    @Test
    void failuresBeyondRetention_dropTheInstance() {
        when(delegate.capabilities(GAME_B)).thenReturn(Optional.of(CAPABLE))
                .thenThrow(new ServiceInstanceClientException("401", null));

        mode.capabilities(GAME_B);
        clock.advance(Duration.ofMinutes(3));

        assertThat(mode.capabilities(GAME_B)).isEmpty();
    }

    @Test
    void neverReadInstance_keepsAxonBehaviour() {
        when(delegate.capabilities(GAME_B)).thenThrow(new ServiceInstanceClientException("401", null))
                .thenReturn(Optional.of(DefaultMemberCapabilities.INCAPABLE_MEMBER));

        assertThat(mode.capabilities(GAME_B)).isEmpty();
        assertThat(mode.capabilities(GAME_B)).contains(DefaultMemberCapabilities.INCAPABLE_MEMBER);
    }

    private static final class MutableClock extends Clock {

        private Instant now = Instant.parse("2026-11-18T22:00:00Z");

        void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
