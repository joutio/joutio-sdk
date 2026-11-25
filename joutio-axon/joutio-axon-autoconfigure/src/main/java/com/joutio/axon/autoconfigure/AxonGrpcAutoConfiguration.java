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
import com.joutio.axon.autoconfigure.metrics.MicrometerQueryBusMetrics;
import com.joutio.axon.grpc.query.EventEnvelopeModule;
import com.joutio.axon.grpc.query.GrpcDistributedQueryBus;
import com.joutio.axon.grpc.query.QueryBusMetrics;
import com.joutio.axon.grpc.query.QueryCodec;
import com.joutio.axon.grpc.query.QueryGrpcService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.ServerInterceptors;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.axonframework.commandhandling.distributed.DistributedCommandBus;
import org.axonframework.queryhandling.DefaultQueryBusSpanFactory;
import org.axonframework.queryhandling.QueryBus;
import org.axonframework.queryhandling.QueryBusSpanFactory;
import org.axonframework.serialization.RevisionResolver;
import org.axonframework.serialization.json.JacksonSerializer;
import org.axonframework.springboot.autoconfig.AxonAutoConfiguration;
import org.axonframework.tracing.NoOpSpanFactory;
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
import org.springframework.context.annotation.Primary;
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
 * Bus gRPC entre pods ({@code docs/backend/axon.md#bus-grpc-entre-pods}) : serveur gRPC du pod, canaux vers les
 * pairs, membership, sécurité M2M et bus de requêtes ({@link GrpcDistributedQueryBus}, {@link QueryGrpcService}).
 *
 * <p>Le bus de commandes passe encore par axon-springcloud ({@code sdk-grpc-commandes}).</p>
 */
@AutoConfiguration(after = AxonAutoConfiguration.class)
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
                                                   ObjectProvider<GrpcDistributedQueryBus> queries) {
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
                GrpcDistributedQueryBus bus = queries.getIfAvailable();
                return bus == null ? Set.of() : bus.localQueryNames();
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

    /**
     * Codec du bus de requêtes : sérialiseur Jackson sur une copie de l'{@link ObjectMapper} du transport Axon
     * (tolérant aux constantes d'énumération inconnues, {@link AxonObjectMapperAutoConfiguration}), enrichie du codec
     * {@link EventEnvelopeModule}. Il n'est pas exposé comme {@code Serializer} : les sérialiseurs général, message et
     * événement d'Axon (format de l'event store) restent inchangés.
     */
    @Bean
    public QueryCodec grpcQueryCodec(@Qualifier(AxonObjectMapperAutoConfiguration.AXON_OBJECT_MAPPER) ObjectMapper objectMapper,
                                     RevisionResolver revisionResolver,
                                     LocalMember localMember) {
        ObjectMapper queryObjectMapper = objectMapper.copy();
        queryObjectMapper.registerModule(new EventEnvelopeModule());
        return new QueryCodec(JacksonSerializer.builder()
                .objectMapper(queryObjectMapper)
                .revisionResolver(revisionResolver)
                .build(), localMember);
    }

    @Bean
    public QueryGrpcService grpcQueryService(@Qualifier("localSegment") QueryBus localSegment,
                                             QueryCodec codec,
                                             ObjectProvider<QueryBusSpanFactory> spanFactory) {
        return new QueryGrpcService(localSegment, codec, querySpanFactory(spanFactory));
    }

    @Primary
    @Bean("distributedQueryBus")
    public GrpcDistributedQueryBus distributedQueryBus(@Qualifier("localSegment") QueryBus localSegment,
                                                       Membership membership,
                                                       PeerChannels channels,
                                                       QueryCodec codec,
                                                       ObjectProvider<QueryBusSpanFactory> spanFactory,
                                                       ObjectProvider<MeterRegistry> meterRegistry,
                                                       AxonGrpcProperties properties) {
        QueryBusMetrics metrics = meterRegistry.getIfAvailable() == null ? QueryBusMetrics.NONE
                : new MicrometerQueryBusMetrics(meterRegistry.getObject());
        return new GrpcDistributedQueryBus(localSegment, membership, channels, codec, querySpanFactory(spanFactory),
                properties.queryTimeout(), metrics);
    }

    @Bean
    public GrpcServer grpcServer(AxonGrpcProperties properties,
                                 MembershipGrpcService membershipService,
                                 QueryGrpcService queryService,
                                 BearerTokenServerInterceptor security,
                                 @Qualifier("grpcExecutor") ExecutorService executor) {
        return new GrpcServer(NettyServerBuilder.forPort(properties.port())
                .executor(executor)
                .addService(ServerInterceptors.intercept(membershipService, security))
                .addService(ServerInterceptors.intercept(queryService, security)),
                () -> {
                    membershipService.leave();
                    queryService.leave();
                },
                properties.shutdownGrace());
    }

    private static QueryBusSpanFactory querySpanFactory(ObjectProvider<QueryBusSpanFactory> spanFactory) {
        return spanFactory.getIfAvailable(() -> DefaultQueryBusSpanFactory.builder()
                .spanFactory(NoOpSpanFactory.INSTANCE)
                .build());
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

    /** Jauges enregistrées une fois l'application prête, jamais pendant l'initialisation d'Axon. */
    @Bean
    public ApplicationListener<ApplicationReadyEvent> grpcMembershipMetrics(Membership membership,
                                                                           GrpcDistributedQueryBus queryBus,
                                                                           QueryGrpcService queryService,
                                                                           ObjectProvider<MeterRegistry> registry) {
        return event -> registry.ifAvailable(meters -> {
            Gauge.builder("joutio.axon.grpc.members", membership, current -> current.topology().members().size())
                    .description("Membres du bus gRPC vus par ce pod")
                    .register(meters);
            Gauge.builder("joutio.axon.subscription.active", queryBus, GrpcDistributedQueryBus::activeSubscriptions)
                    .description("Abonnements de requêtes ouverts par ce pod")
                    .tag("role", "subscriber")
                    .register(meters);
            Gauge.builder("joutio.axon.subscription.active", queryService, QueryGrpcService::activeSubscriptions)
                    .description("Abonnements de requêtes servis par ce pod")
                    .tag("role", "provider")
                    .register(meters);
        });
    }
}
