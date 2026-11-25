package com.joutio.axon.autoconfigure;

import com.joutio.axon.grpc.GrpcEndpoint;
import com.joutio.axon.grpc.GrpcServer;
import com.joutio.axon.grpc.LocalMember;
import com.joutio.axon.grpc.PeerChannels;
import com.joutio.axon.grpc.membership.LocalCapabilities;
import com.joutio.axon.grpc.membership.Membership;
import com.joutio.axon.grpc.membership.MembershipGrpcService;
import com.joutio.axon.grpc.security.BearerTokenClientInterceptor;
import com.joutio.axon.grpc.security.BearerTokenServerInterceptor;
import com.joutio.axon.query.QueryCapabilityRegistry;
import io.grpc.ServerInterceptors;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.axonframework.commandhandling.distributed.DistributedCommandBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.cloud.client.discovery.event.HeartbeatEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.security.oauth2.client.OAuth2AuthorizeRequest;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClient;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientManager;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.util.StringUtils;

import java.time.Clock;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Socle du bus gRPC entre pods ({@code docs/backend/axon.md#bus-grpc-entre-pods}) : serveur gRPC du pod,
 * canaux vers les pairs, membership et sécurité M2M.
 *
 * <p>Les bus de commandes et de requêtes passent encore par HTTP : ce socle annonce et lit les capacités
 * de chaque pod, sans encore router de message.</p>
 */
@AutoConfiguration(after = AxonDistributedQueryAutoConfiguration.class)
@EnableConfigurationProperties(AxonGrpcProperties.class)
public class AxonGrpcAutoConfiguration {

    /** Client M2M partagé par tous les services ({@code joutio-local.yml}, {@code joutio-oauth-client-config}). */
    static final String M2M_REGISTRATION = "server-client";

    private static final Logger logger = LoggerFactory.getLogger(AxonGrpcAutoConfiguration.class);

    @Bean
    public LocalMember grpcLocalMember(Environment environment, AxonGrpcProperties properties) {
        String componentName = environment.getProperty("spring.application.name", "axon-distributed-application");
        GrpcEndpoint endpoint = new GrpcEndpoint(LocalAddress.resolve(environment), properties.port());
        String clientId = StringUtils.hasText(properties.clientId()) ? properties.clientId()
                : environment.getProperty("HOSTNAME", componentName + "@" + endpoint);
        LocalMember member = new LocalMember(clientId, componentName, endpoint);
        logger.info("gRPC bus member: clientId={}, component={}, endpoint={}", clientId, componentName, endpoint);
        return member;
    }

    @Bean
    public LocalCapabilities grpcLocalCapabilities(ObjectProvider<LocalCommandCapabilities> commands,
                                                   ObjectProvider<QueryCapabilityRegistry> queries) {
        return new LocalCapabilities() {
            @Override
            public int loadFactor() {
                LocalCommandCapabilities local = commands.getIfAvailable();
                return local == null ? DistributedCommandBus.INITIAL_LOAD_FACTOR : local.loadFactor();
            }

            @Override
            public Set<String> commandNames() {
                LocalCommandCapabilities local = commands.getIfAvailable();
                return local == null ? Set.of() : local.commandNames();
            }

            @Override
            public Set<String> queryNames() {
                QueryCapabilityRegistry registry = queries.getIfAvailable();
                return registry == null ? Set.of() : registry.snapshot();
            }
        };
    }

    @Bean(destroyMethod = "close")
    public ExecutorService grpcExecutor() {
        return Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("joutio-grpc-", 0).factory());
    }

    @Bean
    public BearerTokenClientInterceptor grpcBearerTokenClientInterceptor(OAuth2AuthorizedClientManager clientManager) {
        OAuth2AuthorizeRequest request = OAuth2AuthorizeRequest.withClientRegistrationId(M2M_REGISTRATION)
                .principal("joutio-axon-grpc")
                .build();
        return new BearerTokenClientInterceptor(() -> {
            OAuth2AuthorizedClient client = clientManager.authorize(request);
            if (client == null) {
                throw new IllegalStateException("No M2M token for " + M2M_REGISTRATION);
            }
            return client.getAccessToken().getTokenValue();
        });
    }

    /**
     * N'accepte que le jeton M2M des services : signature et validité par le {@link JwtDecoder} du pod, puis
     * sujet égal au client M2M ({@code client_credentials} n'a pas d'utilisateur). Un jeton de joueur est refusé.
     */
    @Bean
    public BearerTokenServerInterceptor grpcBearerTokenServerInterceptor(JwtDecoder jwtDecoder,
                                                                         ClientRegistrationRepository registrations) {
        ClientRegistration registration = registrations.findByRegistrationId(M2M_REGISTRATION);
        if (registration == null) {
            throw new IllegalStateException("OAuth2 client registration '" + M2M_REGISTRATION
                    + "' is required by the gRPC bus");
        }
        String m2mSubject = registration.getClientId();
        return new BearerTokenServerInterceptor(token -> {
            Jwt jwt = jwtDecoder.decode(token);
            if (!m2mSubject.equals(jwt.getSubject())) {
                throw new IllegalArgumentException("not an M2M token");
            }
        });
    }

    @Bean
    public MembershipGrpcService grpcMembershipService(LocalMember localMember, LocalCapabilities localCapabilities) {
        return new MembershipGrpcService(localMember, localCapabilities);
    }

    @Bean
    public GrpcServer grpcServer(AxonGrpcProperties properties,
                                 MembershipGrpcService membershipService,
                                 BearerTokenServerInterceptor security,
                                 @Qualifier("grpcExecutor") ExecutorService executor) {
        return new GrpcServer(NettyServerBuilder.forPort(properties.port())
                .executor(executor)
                .addService(ServerInterceptors.intercept(membershipService, security)),
                membershipService,
                properties.shutdownGrace());
    }

    @Bean
    public PeerChannels grpcPeerChannels(BearerTokenClientInterceptor credentials,
                                         @Qualifier("grpcExecutor") ExecutorService executor) {
        return new PeerChannels(endpoint -> NettyChannelBuilder.forAddress(endpoint.host(), endpoint.port())
                .usePlaintext()
                .executor(executor)
                .intercept(credentials)
                .build());
    }

    @Bean
    public Membership grpcMembership(LocalMember localMember,
                                     LocalCapabilities localCapabilities,
                                     DiscoveryClient discoveryClient,
                                     PeerChannels channels,
                                     AxonGrpcProperties properties) {
        return new Membership(localMember, localCapabilities, discoveryClient, channels,
                properties.membershipRefresh(), properties.capabilitiesTimeout(), properties.capabilitiesRetention(),
                Clock.systemUTC());
    }

    /** Un changement de découverte (pod ajouté ou retiré) relit aussitôt les capacités. */
    @Bean
    public ApplicationListener<HeartbeatEvent> grpcMembershipHeartbeatListener(Membership membership) {
        return event -> membership.refreshSoon();
    }

    /** Jauge enregistrée une fois l'application prête, jamais pendant l'initialisation d'Axon. */
    @Bean
    public ApplicationListener<ApplicationReadyEvent> grpcMembershipMetrics(Membership membership,
                                                                           ObjectProvider<MeterRegistry> registry) {
        return event -> registry.ifAvailable(meters -> Gauge.builder("joutio.axon.grpc.members",
                        membership, current -> current.topology().members().size())
                .description("Membres du bus gRPC vus par ce pod")
                .register(meters));
    }
}
