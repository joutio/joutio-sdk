package com.joutio.axon.grpc.membership;

import com.joutio.axon.grpc.GrpcEndpoint;
import com.joutio.axon.grpc.LocalMember;
import com.joutio.axon.grpc.PeerChannels;
import com.joutio.axon.grpc.security.BearerTokenClientInterceptor;
import com.joutio.axon.grpc.security.BearerTokenServerInterceptor;
import com.joutio.axon.grpc.membership.proto.CapabilitiesRequest;
import com.joutio.axon.grpc.membership.proto.MembershipServiceGrpc;
import io.grpc.ClientInterceptor;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MembershipTest {

    private static final Duration RETENTION = Duration.ofMinutes(2);
    private static final String VALID_TOKEN = "m2m";

    private final MutableClock clock = new MutableClock();
    private final FakeDiscovery discovery = new FakeDiscovery();
    private final List<Server> servers = new ArrayList<>();
    private final List<PeerChannels> channelPools = new ArrayList<>();

    @AfterEach
    void tearDown() {
        servers.forEach(Server::shutdownNow);
        channelPools.forEach(PeerChannels::close);
    }

    @Test
    void sees_its_peers_and_their_capabilities() {
        Pod game = pod("game-1", "joutio-game", 1, Set.of("StartGameCommand"), Set.of("GetGameQuery"));
        Pod profile = pod("profile-1", "joutio-profile", 2, Set.of(), Set.of("GetProfileQuery"));

        Membership membership = membershipOf(game);
        membership.refresh();

        Topology topology = membership.topology();
        assertThat(topology.members()).extracting(Member::clientId).containsExactly("game-1", "profile-1");
        assertThat(topology.commandHandlers("StartGameCommand")).extracting(Member::clientId).containsExactly("game-1");
        assertThat(topology.queryHandlers("GetProfileQuery")).extracting(Member::clientId).containsExactly("profile-1");
        assertThat(topology.members().get(1).local()).isFalse();
        assertThat(profile.member.clientId()).isEqualTo("profile-1");
    }

    @Test
    void names_the_local_member_as_its_peers_name_it() {
        Pod game = pod("game-1", "joutio-game", 1, Set.of("StartGameCommand"), Set.of());
        Pod replica = pod("game-2", "joutio-game", 2, Set.of("StartGameCommand"), Set.of());

        Membership fromGame = membershipOf(game);
        Membership fromReplica = membershipOf(replica);
        fromGame.refresh();
        fromReplica.refresh();

        Member selfSeenByGame = fromGame.topology().members().getFirst();
        Member selfSeenByReplica = fromReplica.topology().members().getFirst();
        assertThat(selfSeenByGame.local()).isTrue();
        assertThat(selfSeenByReplica.local()).isFalse();
        assertThat(selfSeenByGame.clientId()).isEqualTo(selfSeenByReplica.clientId());
        assertThat(selfSeenByGame.endpoint()).isEqualTo(selfSeenByReplica.endpoint());
        assertThat(fromGame.topology().members()).extracting(Member::endpoint)
                .isEqualTo(fromReplica.topology().members().stream().map(Member::endpoint).toList());
    }

    @Test
    void keeps_the_last_known_capabilities_of_a_discovered_peer_during_the_retention() {
        Pod game = pod("game-1", "joutio-game", 1, Set.of(), Set.of());
        Pod profile = pod("profile-1", "joutio-profile", 2, Set.of(), Set.of("GetProfileQuery"));
        Membership membership = membershipOf(game);
        membership.refresh();

        profile.server.shutdownNow();
        clock.advance(RETENTION.minusSeconds(1));
        membership.refresh();
        assertThat(membership.topology().queryHandlers("GetProfileQuery")).hasSize(1);

        clock.advance(Duration.ofSeconds(2));
        membership.refresh();
        assertThat(membership.topology().queryHandlers("GetProfileQuery")).isEmpty();
    }

    @Test
    void removes_a_peer_that_is_no_longer_discovered() {
        Pod game = pod("game-1", "joutio-game", 1, Set.of(), Set.of());
        Pod profile = pod("profile-1", "joutio-profile", 2, Set.of(), Set.of("GetProfileQuery"));
        Membership membership = membershipOf(game);
        membership.refresh();

        discovery.remove(profile.member.endpoint());
        membership.refresh();

        assertThat(membership.topology().members()).extracting(Member::clientId).containsExactly("game-1");
    }

    @Test
    void removes_a_leaving_peer_without_retention() {
        Pod game = pod("game-1", "joutio-game", 1, Set.of(), Set.of());
        Pod profile = pod("profile-1", "joutio-profile", 2, Set.of(), Set.of("GetProfileQuery"));
        Membership membership = membershipOf(game);
        membership.refresh();

        profile.service.leave();
        membership.refresh();

        assertThat(membership.topology().members()).extracting(Member::clientId).containsExactly("game-1");
    }

    @Test
    void notifies_listeners_only_when_the_topology_changes() {
        Pod game = pod("game-1", "joutio-game", 1, Set.of(), Set.of());
        pod("profile-1", "joutio-profile", 2, Set.of(), Set.of("GetProfileQuery"));
        Membership membership = membershipOf(game);
        List<Topology> seen = new ArrayList<>();
        membership.addListener(seen::add);

        membership.refresh();
        clock.advance(Duration.ofSeconds(10));
        membership.refresh();

        assertThat(seen).hasSize(2);
        assertThat(seen.getLast().members()).hasSize(2);
    }

    @Test
    void refuses_a_call_without_a_valid_token() {
        Pod game = pod("game-1", "joutio-game", 1, Set.of(), Set.of());
        Pod profile = pod("profile-1", "joutio-profile", 2, Set.of(), Set.of("GetProfileQuery"));

        assertThatThrownBy(() -> MembershipServiceGrpc.newBlockingStub(
                        InProcessChannelBuilder.forName(profile.member.endpoint().toString()).build())
                .capabilities(CapabilitiesRequest.getDefaultInstance()))
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED));

        Membership withBadToken = membershipOf(game, new BearerTokenClientInterceptor(() -> "player-token"));
        withBadToken.refresh();
        assertThat(withBadToken.topology().members()).extracting(Member::clientId).containsExactly("game-1");
    }

    private Pod pod(String clientId, String component, int port, Set<String> commands, Set<String> queries) {
        LocalMember member = new LocalMember(clientId, component, new GrpcEndpoint("10.0.0." + port, 8124));
        MembershipGrpcService service = new MembershipGrpcService(member, new StaticCapabilities(commands, queries));
        BearerTokenServerInterceptor security = new BearerTokenServerInterceptor(token -> {
            if (!VALID_TOKEN.equals(token)) {
                throw new IllegalArgumentException("not an M2M token");
            }
        });
        try {
            Server server = InProcessServerBuilder.forName(member.endpoint().toString())
                    .directExecutor()
                    .addService(ServerInterceptors.intercept(service, security))
                    .build()
                    .start();
            servers.add(server);
            discovery.add(component, member.endpoint());
            return new Pod(member, service, server);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private Membership membershipOf(Pod pod) {
        return membershipOf(pod, new BearerTokenClientInterceptor(() -> VALID_TOKEN));
    }

    private Membership membershipOf(Pod pod, ClientInterceptor credentials) {
        PeerChannels channels = new PeerChannels(endpoint -> InProcessChannelBuilder.forName(endpoint.toString())
                .directExecutor()
                .intercept(credentials)
                .build());
        channelPools.add(channels);
        return new Membership(pod.member, pod.service.localCapabilities(), discovery, channels,
                Duration.ofSeconds(10), Duration.ofSeconds(2), RETENTION, clock);
    }

    private record Pod(LocalMember member, MembershipGrpcService service, Server server) {
    }

    private record StaticCapabilities(Set<String> commandNames, Set<String> queryNames) implements LocalCapabilities {

        @Override
        public int loadFactor() {
            return 100;
        }
    }

    private static final class FakeDiscovery implements DiscoveryClient {

        private final Map<GrpcEndpoint, ServiceInstance> instances = new LinkedHashMap<>();

        void add(String service, GrpcEndpoint endpoint) {
            instances.put(endpoint, new DefaultServiceInstance(endpoint.toString(), service, endpoint.host(), 8080,
                    false, Map.of(GrpcEndpoint.PORT_METADATA, String.valueOf(endpoint.port()))));
        }

        void remove(GrpcEndpoint endpoint) {
            instances.remove(endpoint);
        }

        @Override
        public String description() {
            return "fake";
        }

        @Override
        public List<ServiceInstance> getInstances(String serviceId) {
            return instances.values().stream().filter(instance -> instance.getServiceId().equals(serviceId)).toList();
        }

        @Override
        public List<String> getServices() {
            return instances.values().stream().map(ServiceInstance::getServiceId).distinct().toList();
        }
    }

    private static final class MutableClock extends Clock {

        private Instant now = Instant.parse("2026-11-25T10:00:00Z");

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
