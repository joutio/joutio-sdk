package com.joutio.axon.autoconfigure;

import com.joutio.axon.autoconfigure.AxonDistributedFallbackRegistrationAutoConfiguration.SimpleRegistration;
import org.axonframework.commandhandling.GenericCommandMessage;
import org.axonframework.commandhandling.distributed.Member;
import org.axonframework.commandhandling.distributed.commandfilter.AcceptAll;
import org.axonframework.extensions.springcloud.commandhandling.SpringCloudCommandRouter;
import org.axonframework.extensions.springcloud.commandhandling.mode.CapabilityDiscoveryMode;
import org.axonframework.extensions.springcloud.commandhandling.mode.DefaultMemberCapabilities;
import org.axonframework.extensions.springcloud.commandhandling.mode.MemberCapabilities;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.cloud.client.discovery.event.HeartbeatEvent;
import org.springframework.cloud.client.discovery.event.InstanceRegisteredEvent;
import org.springframework.cloud.client.serviceregistry.Registration;
import org.springframework.context.support.StaticApplicationContext;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Deux répliques de game : le BFF et chaque réplique doivent router une même partie vers le même pod.
 *
 * <p>Un {@code SpringCloudCommandRouter} se nomme {@code SERVICE[LOCAL]} dans son propre anneau tant
 * qu'il n'a pas reçu d'{@code InstanceRegisteredEvent}, alors que les autres le nomment par son URI :
 * les positions dans l'anneau diffèrent et deux pods traitent le même agrégat. Ni la découverte
 * Kubernetes ni la Simple Discovery ne publient cet événement : le SDK le publie.</p>
 */
class MultiReplicaRoutingTest {

    private static final ServiceInstance GAME_A = instance("10.42.0.1");
    private static final ServiceInstance GAME_B = instance("10.42.1.2");

    @Test
    void unregisteredReplica_disagreesWithTheBffOnSomeGames() {
        SpringCloudCommandRouter bff = router(new SimpleRegistration("joutio-bff", "10.42.0.9", 8080));
        SpringCloudCommandRouter gameA = router(registrationOf(GAME_A));

        assertThat(disagreements(bff, gameA)).isPositive();
    }

    @Test
    void registeredReplica_agreesWithTheBffOnEveryGame() {
        SpringCloudCommandRouter bff = router(new SimpleRegistration("joutio-bff", "10.42.0.9", 8080));
        SpringCloudCommandRouter gameA = router(registrationOf(GAME_A));

        gameA.resetLocalMembership(new InstanceRegisteredEvent<>(this, registrationOf(GAME_A)));

        assertThat(disagreements(bff, gameA)).isZero();
    }

    @Test
    void sdk_publishesTheRegistrationOnceTheApplicationIsReady() {
        AtomicReference<Object> published = new AtomicReference<>();
        try (StaticApplicationContext context = new StaticApplicationContext()) {
            context.addApplicationListener(event -> {
                if (event instanceof InstanceRegisteredEvent<?> registered) {
                    published.set(registered.getConfig());
                }
            });
            context.refresh();
            SimpleRegistration registration = registrationOf(GAME_A);

            new AxonDistributedFallbackRegistrationAutoConfiguration.FallbackRegistrationPublisher(registration, context)
                    .onApplicationEvent(new ApplicationReadyEvent(
                            new SpringApplication(), new String[0], context, java.time.Duration.ZERO));

            assertThat(published.get()).isEqualTo(registration);
        }
    }

    /** Nombre de parties (sur 200) que les deux routeurs n'envoient pas au même pod. */
    private static long disagreements(SpringCloudCommandRouter left, SpringCloudCommandRouter right) {
        return IntStream.range(0, 200)
                .mapToObj(i -> "game-" + i)
                .filter(gameId -> !destination(left, gameId).equals(destination(right, gameId)))
                .count();
    }

    private static URI destination(SpringCloudCommandRouter router, String gameId) {
        GameCommand command = new GameCommand(gameId);
        Member member = router.findDestination(new GenericCommandMessage<>(command)).orElseThrow();
        return member.local() && member.getConnectionEndpoint(URI.class).isEmpty()
                ? GAME_A.getUri()
                : member.getConnectionEndpoint(URI.class).orElse(GAME_A.getUri());
    }

    private static SpringCloudCommandRouter router(Registration local) {
        SpringCloudCommandRouter router = SpringCloudCommandRouter.builder()
                .discoveryClient(discovery())
                .localServiceInstance(local)
                .routingStrategy(command -> ((GameCommand) command.getPayload()).gameId())
                .capabilityDiscoveryMode(new GameOnlyCapabilities())
                .serializer(XStreamSerializer.defaultSerializer())
                .build();
        router.updateMembership(100, AcceptAll.INSTANCE);
        router.updateMemberships(new HeartbeatEvent(router, 1));
        return router;
    }

    private static SimpleRegistration registrationOf(ServiceInstance instance) {
        return new SimpleRegistration(instance.getServiceId(), instance.getHost(), instance.getPort());
    }

    private static ServiceInstance instance(String host) {
        return new DefaultServiceInstance("joutio-game-" + host, "joutio-game", host, 8080, false);
    }

    private static DiscoveryClient discovery() {
        DiscoveryClient client = mock(DiscoveryClient.class);
        org.mockito.Mockito.when(client.getServices()).thenReturn(List.of("joutio-game"));
        org.mockito.Mockito.when(client.getInstances("joutio-game")).thenReturn(List.of(GAME_A, GAME_B));
        return client;
    }

    /** Seules les répliques de game acceptent les commandes de partie. */
    private static final class GameOnlyCapabilities implements CapabilityDiscoveryMode {

        @Override
        public void updateLocalCapabilities(ServiceInstance localInstance, int loadFactor,
                                            org.axonframework.commandhandling.distributed.CommandMessageFilter filter) {
            // Capacités fournies par capabilities(...).
        }

        @Override
        public Optional<MemberCapabilities> capabilities(ServiceInstance serviceInstance) {
            return Optional.of(new DefaultMemberCapabilities(100, AcceptAll.INSTANCE));
        }
    }

    record GameCommand(String gameId) {
    }
}
