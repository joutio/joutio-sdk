package com.joutio.axon.grpc.query;

import com.joutio.axon.grpc.GrpcEndpoint;
import com.joutio.axon.grpc.LocalMember;
import com.joutio.axon.grpc.PeerChannels;
import com.joutio.axon.grpc.membership.LocalCapabilities;
import com.joutio.axon.grpc.membership.Membership;
import com.joutio.axon.grpc.membership.MembershipGrpcService;
import io.axoniq.axonserver.grpc.query.QueryServiceGrpc;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.axonframework.messaging.responsetypes.ResponseTypes;
import org.axonframework.queryhandling.DefaultQueryBusSpanFactory;
import org.axonframework.queryhandling.GenericQueryMessage;
import org.axonframework.queryhandling.GenericStreamingQueryMessage;
import org.axonframework.queryhandling.GenericSubscriptionQueryMessage;
import org.axonframework.queryhandling.NoHandlerForQueryException;
import org.axonframework.queryhandling.QueryExecutionException;
import org.axonframework.queryhandling.QueryHandler;
import org.axonframework.queryhandling.QueryResponseMessage;
import org.axonframework.queryhandling.SimpleQueryBus;
import org.axonframework.queryhandling.SubscriptionQueryResult;
import org.axonframework.queryhandling.SubscriptionQueryUpdateMessage;
import org.axonframework.queryhandling.annotation.AnnotationQueryHandlerAdapter;
import org.axonframework.tracing.NoOpSpanFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Bus de requêtes gRPC entre pods, en in-process : point à point, scatter-gather, streaming avec contre-pression et
 * abonnements fusionnés sur plusieurs répliques.
 */
class GrpcQueryBusTest {

    record GetScore(String gameId) {
    }

    record Score(String gameId, int value) {
    }

    record ListScores(int count) {
    }

    record Fail(String reason) {
    }

    record Slow() {
    }

    private static final Duration TIMEOUT = Duration.ofMillis(500);

    private final FakeDiscovery discovery = new FakeDiscovery();
    private final List<Pod> pods = new ArrayList<>();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicInteger produced = new AtomicInteger();

    @AfterEach
    void tearDown() {
        pods.forEach(Pod::stop);
        executor.shutdownNow();
    }

    @Test
    void sends_a_query_to_a_capable_peer() throws Exception {
        pod("game-1", 1, new ScoreHandler(7));
        Pod bff = pod("bff-1", 9);
        refreshAll();

        QueryResponseMessage<Score> response = bff.bus.query(
                new GenericQueryMessage<>(new GetScore("g"), ResponseTypes.instanceOf(Score.class))).get();

        assertThat(response.getPayload()).isEqualTo(new Score("g", 7));
    }

    @Test
    void spreads_queries_over_the_capable_replicas() throws Exception {
        pod("game-1", 1, new ScoreHandler(1));
        pod("game-2", 2, new ScoreHandler(2));
        Pod bff = pod("bff-1", 9);
        refreshAll();

        Set<Integer> answeredBy = new java.util.HashSet<>();
        for (int i = 0; i < 4; i++) {
            answeredBy.add(bff.bus.query(new GenericQueryMessage<>(new GetScore("g"),
                    ResponseTypes.instanceOf(Score.class))).get().getPayload().value());
        }

        assertThat(answeredBy).containsExactlyInAnyOrder(1, 2);
    }

    @Test
    void business_problem_reaches_the_dispatcher_intact() throws Exception {
        pod("game-1", 1, new ScoreHandler(7));
        Pod bff = pod("bff-1", 9);
        refreshAll();

        QueryResponseMessage<Score> response = bff.bus.query(
                new GenericQueryMessage<>(new Fail("closed"), ResponseTypes.instanceOf(Score.class))).get();

        assertThat(response.isExceptional()).isTrue();
        assertThat(response.exceptionResult()).isInstanceOfSatisfying(QueryExecutionException.class,
                failure -> assertThat(failure.getDetails()).contains(Map.of("type", "game/closed", "detail", "closed")));
    }

    @Test
    void fails_without_any_capable_member() {
        Pod bff = pod("bff-1", 9);
        refreshAll();

        assertThatThrownBy(() -> bff.bus.query(
                new GenericQueryMessage<>(new GetScore("g"), ResponseTypes.instanceOf(Score.class))).get())
                .hasCauseInstanceOf(NoHandlerForQueryException.class);
    }

    @Test
    void a_frozen_peer_fails_the_query_within_the_timeout() {
        pod("game-1", 1, new ScoreHandler(7));
        Pod bff = pod("bff-1", 9);
        refreshAll();

        long start = System.nanoTime();
        assertThatThrownBy(() -> bff.bus.query(
                new GenericQueryMessage<>(new Slow(), ResponseTypes.instanceOf(Score.class))).get())
                .isInstanceOf(ExecutionException.class)
                .cause().isInstanceOf(QueryExecutionException.class)
                .hasCauseInstanceOf(TimeoutException.class);
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(2));
    }

    @Test
    void scatter_gather_asks_every_capable_peer() {
        pod("game-1", 1, new ScoreHandler(1));
        pod("game-2", 2, new ScoreHandler(2));
        Pod bff = pod("bff-1", 9);
        refreshAll();

        List<Integer> values = bff.bus.scatterGather(
                        new GenericQueryMessage<>(new GetScore("g"), ResponseTypes.instanceOf(Score.class)),
                        1, java.util.concurrent.TimeUnit.SECONDS)
                .map(response -> response.getPayload().value())
                .toList();

        assertThat(values).containsExactlyInAnyOrder(1, 2);
    }

    @Test
    void streaming_query_follows_the_demand_of_the_subscriber() throws Exception {
        pod("game-1", 1, new ScoreHandler(7));
        Pod bff = pod("bff-1", 9);
        refreshAll();

        List<Integer> received = new CopyOnWriteArrayList<>();
        AtomicReference<Subscription> subscription = new AtomicReference<>();
        AtomicReference<Boolean> completed = new AtomicReference<>(false);
        Flux.from(bff.bus.streamingQuery(new GenericStreamingQueryMessage<>(new ListScores(50), Score.class)))
                .subscribe(new BaseSubscriber<QueryResponseMessage<Score>>() {
                    @Override
                    protected void hookOnSubscribe(Subscription s) {
                        subscription.set(s);
                        s.request(3);
                    }

                    @Override
                    protected void hookOnNext(QueryResponseMessage<Score> value) {
                        received.add(value.getPayload().value());
                    }

                    @Override
                    protected void hookOnComplete() {
                        completed.set(true);
                    }
                });

        eventually(() -> assertThat(received).hasSize(3));
        Thread.sleep(200);
        assertThat(received).hasSize(3);
        assertThat(produced.get()).isLessThan(10);

        subscription.get().request(Long.MAX_VALUE);
        eventually(() -> assertThat(completed.get()).isTrue());
        assertThat(received).hasSize(50).startsWith(0, 1, 2);
    }

    @Test
    void subscription_merges_the_updates_of_every_replica() {
        Pod game1 = pod("game-1", 1, new ScoreHandler(1));
        Pod game2 = pod("game-2", 2, new ScoreHandler(2));
        Pod bff = pod("bff-1", 9);
        refreshAll();

        SubscriptionQueryResult<QueryResponseMessage<Score>, SubscriptionQueryUpdateMessage<Score>> result =
                subscribe(bff);
        List<Score> updates = collect(result);

        assertThat(result.initialResult().block(Duration.ofSeconds(2)).getPayload().gameId()).isEqualTo("g");
        emit(game2, new Score("g", 20));
        emit(game1, new Score("g", 10));

        eventually(() -> assertThat(updates).containsExactlyInAnyOrder(new Score("g", 20), new Score("g", 10)));
    }

    @Test
    void subscription_includes_a_replica_added_later() {
        pod("game-1", 1, new ScoreHandler(1));
        Pod bff = pod("bff-1", 9);
        refreshAll();
        SubscriptionQueryResult<QueryResponseMessage<Score>, SubscriptionQueryUpdateMessage<Score>> result =
                subscribe(bff);
        List<Score> updates = collect(result);
        result.initialResult().block(Duration.ofSeconds(2));

        Pod game2 = pod("game-2", 2, new ScoreHandler(2));
        refreshAll();
        eventually(() -> assertThat(game2.local.queryUpdateEmitter().activeSubscriptions()).hasSize(1));
        emit(game2, new Score("g", 20));

        eventually(() -> assertThat(updates).containsExactly(new Score("g", 20)));
    }

    @Test
    void cancelling_closes_the_subscription_on_every_replica() {
        Pod game1 = pod("game-1", 1, new ScoreHandler(1));
        Pod game2 = pod("game-2", 2, new ScoreHandler(2));
        Pod bff = pod("bff-1", 9);
        refreshAll();
        SubscriptionQueryResult<QueryResponseMessage<Score>, SubscriptionQueryUpdateMessage<Score>> result =
                subscribe(bff);
        result.initialResult().block(Duration.ofSeconds(2));
        assertThat(game1.local.queryUpdateEmitter().activeSubscriptions()).hasSize(1);
        assertThat(game2.local.queryUpdateEmitter().activeSubscriptions()).hasSize(1);

        result.cancel();

        eventually(() -> {
            assertThat(game1.local.queryUpdateEmitter().activeSubscriptions()).isEmpty();
            assertThat(game2.local.queryUpdateEmitter().activeSubscriptions()).isEmpty();
            assertThat(bff.bus.activeSubscriptions()).isZero();
        });
    }

    @Test
    void losing_one_replica_keeps_the_subscription_and_losing_all_fails_it() {
        Pod game1 = pod("game-1", 1, new ScoreHandler(1));
        Pod game2 = pod("game-2", 2, new ScoreHandler(2));
        Pod bff = pod("bff-1", 9);
        refreshAll();
        SubscriptionQueryResult<QueryResponseMessage<Score>, SubscriptionQueryUpdateMessage<Score>> result =
                subscribe(bff);
        List<Score> updates = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        result.updates().subscribe(update -> updates.add(update.getPayload()), failure::set);
        result.initialResult().block(Duration.ofSeconds(2));

        game1.service.leave();
        emit(game2, new Score("g", 20));
        eventually(() -> assertThat(updates).containsExactly(new Score("g", 20)));
        assertThat(failure.get()).isNull();

        game2.service.leave();
        eventually(() -> assertThat(failure.get()).isInstanceOf(QueryExecutionException.class));
    }

    @Test
    void provider_relays_updates_within_the_granted_permits() {
        Pod game1 = pod("game-1", 1, new ScoreHandler(1));
        QueryCodec codec = QueryCodecTest.codec();
        List<SubscriptionQueryResponse> responses = new CopyOnWriteArrayList<>();
        ManagedChannel channel = InProcessChannelBuilder.forName(game1.member.endpoint().toString()).build();
        try {
            StreamObserver<SubscriptionQueryRequest> requests = QueryServiceGrpc.newStub(channel)
                    .subscription(new StreamObserver<>() {
                        @Override
                        public void onNext(SubscriptionQueryResponse value) {
                            responses.add(value);
                        }

                        @Override
                        public void onError(Throwable t) {
                        }

                        @Override
                        public void onCompleted() {
                        }
                    });
            GenericSubscriptionQueryMessage<GetScore, Score, Score> query = new GenericSubscriptionQueryMessage<>(
                    new GetScore("g"), ResponseTypes.instanceOf(Score.class), ResponseTypes.instanceOf(Score.class));
            requests.onNext(SubscriptionQueryRequest.newBuilder()
                    .setSubscribe(codec.serializeSubscription(query, 2)).build());
            eventually(() -> assertThat(responses).hasSize(1));
            assertThat(responses.getFirst().getResponseCase())
                    .isEqualTo(SubscriptionQueryResponse.ResponseCase.RESPONSE_NOT_SET);

            for (int i = 0; i < 5; i++) {
                emit(game1, new Score("g", i));
            }
            eventually(() -> assertThat(updatesIn(responses)).isEqualTo(2));
            sleep(200);
            assertThat(updatesIn(responses)).isEqualTo(2);

            requests.onNext(SubscriptionQueryRequest.newBuilder()
                    .setFlowControl(SubscriptionQuery.newBuilder().setNumberOfPermits(3)).build());
            eventually(() -> assertThat(updatesIn(responses)).isEqualTo(5));

            requests.onNext(SubscriptionQueryRequest.newBuilder()
                    .setUnsubscribe(SubscriptionQuery.getDefaultInstance()).build());
            eventually(() -> assertThat(game1.local.queryUpdateEmitter().activeSubscriptions()).isEmpty());
        } finally {
            channel.shutdownNow();
        }
    }

    private static long updatesIn(List<SubscriptionQueryResponse> responses) {
        return responses.stream()
                .filter(response -> response.getResponseCase() == SubscriptionQueryResponse.ResponseCase.UPDATE)
                .count();
    }

    private static SubscriptionQueryResult<QueryResponseMessage<Score>, SubscriptionQueryUpdateMessage<Score>> subscribe(
            Pod dispatcher) {
        return dispatcher.bus.subscriptionQuery(new GenericSubscriptionQueryMessage<>(new GetScore("g"),
                ResponseTypes.instanceOf(Score.class), ResponseTypes.instanceOf(Score.class)), 8);
    }

    private static List<Score> collect(
            SubscriptionQueryResult<QueryResponseMessage<Score>, SubscriptionQueryUpdateMessage<Score>> result) {
        List<Score> updates = new CopyOnWriteArrayList<>();
        result.updates().subscribe(update -> updates.add(update.getPayload()));
        return updates;
    }

    private static void emit(Pod pod, Score score) {
        pod.local.queryUpdateEmitter().emit(GetScore.class, query -> query.gameId().equals(score.gameId()), score);
    }

    private void refreshAll() {
        pods.forEach(pod -> pod.membership.refresh());
    }

    private Pod pod(String clientId, int address, Object... handlers) {
        LocalMember member = new LocalMember(clientId, clientId.replaceAll("-\\d+$", ""),
                new GrpcEndpoint("10.0.0." + address, 8124));
        SimpleQueryBus local = SimpleQueryBus.builder().build();
        QueryCodec codec = new QueryCodec(
                org.axonframework.serialization.json.JacksonSerializer.builder()
                        .objectMapper(com.fasterxml.jackson.databind.json.JsonMapper.builder()
                                .addModule(new com.fasterxml.jackson.datatype.jdk8.Jdk8Module())
                                .addModule(new EventEnvelopeModule())
                                .build())
                        .build(), member);
        DefaultQueryBusSpanFactory spanFactory = DefaultQueryBusSpanFactory.builder()
                .spanFactory(NoOpSpanFactory.INSTANCE).build();
        QueryGrpcService service = new QueryGrpcService(local, codec, spanFactory);
        AtomicReference<GrpcDistributedQueryBus> bus = new AtomicReference<>();
        LocalCapabilities capabilities = new LocalCapabilities() {
            @Override
            public int loadFactor() {
                return 100;
            }

            @Override
            public Set<String> commandNames() {
                return Set.of();
            }

            @Override
            public Set<String> queryNames() {
                return bus.get().localQueryNames();
            }
        };
        PeerChannels channels = new PeerChannels(endpoint -> InProcessChannelBuilder.forName(endpoint.toString())
                .executor(executor)
                .build());
        Membership membership = new Membership(member, capabilities, discovery, channels,
                Duration.ofSeconds(10), Duration.ofSeconds(2), Duration.ofMinutes(2), Clock.systemUTC());
        bus.set(new GrpcDistributedQueryBus(local, membership, channels, codec, spanFactory, TIMEOUT,
                QueryBusMetrics.NONE));
        for (Object handler : handlers) {
            new AnnotationQueryHandlerAdapter<>(handler).subscribe(bus.get());
        }
        try {
            Server server = InProcessServerBuilder.forName(member.endpoint().toString())
                    .executor(executor)
                    .addService(new MembershipGrpcService(member, capabilities))
                    .addService(service)
                    .build()
                    .start();
            discovery.add(member.componentName(), member.endpoint());
            Pod pod = new Pod(member, local, service, server, membership, bus.get());
            pods.add(pod);
            return pod;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void eventually(Runnable assertion) {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (true) {
            try {
                assertion.run();
                return;
            } catch (AssertionError e) {
                if (System.nanoTime() > deadline) {
                    throw e;
                }
                sleep(20);
            }
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private record Pod(LocalMember member, SimpleQueryBus local, QueryGrpcService service, Server server,
                       Membership membership, GrpcDistributedQueryBus bus) {

        void stop() {
            server.shutdownNow();
            membership.stop();
        }
    }

    public class ScoreHandler {

        private final int value;

        ScoreHandler(int value) {
            this.value = value;
        }

        @QueryHandler
        public Score handle(GetScore query) {
            return new Score(query.gameId(), value);
        }

        @QueryHandler
        public Score handle(Fail query) {
            throw new QueryExecutionException("refused", null,
                    Map.of("type", "game/" + query.reason(), "detail", query.reason()));
        }

        @QueryHandler
        public Score handle(Slow query) throws InterruptedException {
            Thread.sleep(3000);
            return new Score("slow", value);
        }

        @QueryHandler
        public Flux<Score> handle(ListScores query) {
            return Flux.range(0, query.count())
                    .map(i -> {
                        produced.incrementAndGet();
                        return new Score("g", i);
                    });
        }
    }

    private static final class FakeDiscovery implements DiscoveryClient {

        private final Map<GrpcEndpoint, ServiceInstance> instances = new LinkedHashMap<>();

        synchronized void add(String service, GrpcEndpoint endpoint) {
            instances.put(endpoint, new DefaultServiceInstance(endpoint.toString(), service, endpoint.host(), 8080,
                    false, Map.of(GrpcEndpoint.PORT_METADATA, String.valueOf(endpoint.port()))));
        }

        @Override
        public String description() {
            return "fake";
        }

        @Override
        public synchronized List<ServiceInstance> getInstances(String serviceId) {
            return instances.values().stream().filter(instance -> instance.getServiceId().equals(serviceId)).toList();
        }

        @Override
        public synchronized List<String> getServices() {
            return instances.values().stream().map(ServiceInstance::getServiceId).distinct()
                    .collect(Collectors.toList());
        }
    }
}
