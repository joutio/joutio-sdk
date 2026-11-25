package com.joutio.axon.autoconfigure;

import com.joutio.axon.grpc.GrpcServer;
import com.joutio.axon.grpc.LocalMember;
import com.joutio.axon.grpc.membership.Membership;
import com.joutio.axon.grpc.membership.proto.CapabilitiesRequest;
import com.joutio.axon.grpc.membership.proto.MemberCapabilities;
import com.joutio.axon.grpc.membership.proto.MembershipServiceGrpc;
import com.joutio.axon.grpc.query.GrpcDistributedQueryBus;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.joutio.axon.grpc.security.BearerTokenClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.axonframework.commandhandling.distributed.commandfilter.CommandNameFilter;
import org.axonframework.queryhandling.QueryBus;
import org.axonframework.queryhandling.SimpleQueryBus;
import org.axonframework.serialization.AnnotationRevisionResolver;
import org.axonframework.serialization.RevisionResolver;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.cloud.client.discovery.simple.SimpleDiscoveryClient;
import org.springframework.cloud.client.discovery.simple.SimpleDiscoveryProperties;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientManager;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.client.registration.InMemoryClientRegistrationRepository;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtException;

import java.time.Instant;
import java.util.Set;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class AxonGrpcAutoConfigurationTest {

    private static final JwtDecoder DECODER = token -> switch (token) {
        case "m2m" -> jwt("server");
        case "player" -> jwt("user-42");
        default -> throw new JwtException("bad signature");
    };

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(AxonGrpcAutoConfiguration.class))
            .withPropertyValues("spring.application.name=joutio-game")
            .withBean(DiscoveryClient.class, () -> new SimpleDiscoveryClient(new SimpleDiscoveryProperties()))
            .withBean(JwtDecoder.class, () -> DECODER)
            .withBean(ClientRegistrationRepository.class, () -> new InMemoryClientRegistrationRepository(
                    ClientRegistration.withRegistrationId("server-client")
                            .clientId("server")
                            .authorizationGrantType(AuthorizationGrantType.CLIENT_CREDENTIALS)
                            .tokenUri("http://localhost/oauth2/token")
                            .build()))
            .withBean(OAuth2AuthorizedClientManager.class, () -> mock(OAuth2AuthorizedClientManager.class))
            .withBean("localSegment", QueryBus.class, () -> SimpleQueryBus.builder().build())
            .withBean(AxonObjectMapperAutoConfiguration.AXON_OBJECT_MAPPER, ObjectMapper.class, ObjectMapper::new)
            .withBean(RevisionResolver.class, AnnotationRevisionResolver::new);

    @Test
    void names_the_local_member_after_the_pod_and_its_discovered_address() {
        runner.withPropertyValues("HOSTNAME=joutio-game-7d9f-x2k4", "server.address=10.42.0.17",
                        "joutio.axon.grpc.port=0")
                .run(context -> {
                    LocalMember member = context.getBean(LocalMember.class);
                    assertThat(member.clientId()).isEqualTo("joutio-game-7d9f-x2k4");
                    assertThat(member.componentName()).isEqualTo("joutio-game");
                    assertThat(member.endpoint().host()).isEqualTo("10.42.0.17");
                });
    }

    @Test
    void falls_back_to_the_component_and_address_without_hostname() {
        runner.withPropertyValues("joutio.axon.grpc.port=0")
                .run(context -> assertThat(context.getBean(LocalMember.class).clientId())
                        .isEqualTo("joutio-game@localhost:0"));
    }

    @Test
    void serves_the_local_capabilities_to_an_m2m_caller_only() {
        runner.withPropertyValues("joutio.axon.grpc.port=0")
                .withBean(LocalCommandCapabilities.class, () -> {
                    LocalCommandCapabilities commands = new LocalCommandCapabilities();
                    commands.update(100, new CommandNameFilter(Set.of("StartGameCommand", "AnswerQuestionCommand")));
                    return commands;
                })
                .run(context -> {
                    GrpcDistributedQueryBus queryBus = context.getBean(GrpcDistributedQueryBus.class);
                    queryBus.subscribe("GetGameQuery", String.class, message -> "game");
                    assertThat(context.getBean(QueryBus.class)).isSameAs(queryBus);
                    int port = context.getBean(GrpcServer.class).port();
                    withChannel(port, "m2m", channel -> {
                        MemberCapabilities capabilities = MembershipServiceGrpc.newBlockingStub(channel)
                                .capabilities(CapabilitiesRequest.getDefaultInstance());
                        assertThat(capabilities.getComponentName()).isEqualTo("joutio-game");
                        assertThat(capabilities.getCommandsList())
                                .extracting(command -> command.getCommand() + "/" + command.getLoadFactor())
                                .containsExactly("AnswerQuestionCommand/100", "StartGameCommand/100");
                        assertThat(capabilities.getQueriesList()).extracting(query -> query.getQuery())
                                .containsExactly("GetGameQuery");
                    });
                    for (String refused : new String[]{"player", "forged"}) {
                        withChannel(port, refused, channel -> assertThatThrownBy(() -> MembershipServiceGrpc
                                .newBlockingStub(channel).capabilities(CapabilitiesRequest.getDefaultInstance()))
                                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> assertThat(
                                        e.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED)));
                    }
                    assertThat(context).hasSingleBean(Membership.class);
                });
    }

    private static void withChannel(int port, String token, Consumer<ManagedChannel> body) {
        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", port)
                .usePlaintext()
                .intercept(new BearerTokenClientInterceptor(() -> token))
                .build();
        try {
            body.accept(channel);
        } finally {
            channel.shutdownNow();
        }
    }

    private static Jwt jwt(String subject) {
        return Jwt.withTokenValue("token").header("alg", "RS256").subject(subject)
                .issuedAt(Instant.now()).expiresAt(Instant.now().plusSeconds(60)).build();
    }
}
