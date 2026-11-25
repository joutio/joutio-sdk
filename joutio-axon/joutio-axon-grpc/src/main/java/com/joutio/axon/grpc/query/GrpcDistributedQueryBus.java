package com.joutio.axon.grpc.query;

import com.joutio.axon.grpc.PeerChannels;
import com.joutio.axon.grpc.membership.Member;
import com.joutio.axon.grpc.membership.Membership;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.QueryServiceGrpc;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.axonframework.common.Registration;
import org.axonframework.messaging.GenericMessage;
import org.axonframework.messaging.MessageDispatchInterceptor;
import org.axonframework.messaging.MessageHandler;
import org.axonframework.messaging.MessageHandlerInterceptor;
import org.axonframework.messaging.responsetypes.ResponseType;
import org.axonframework.messaging.responsetypes.ResponseTypes;
import org.axonframework.queryhandling.DefaultSubscriptionQueryResult;
import org.axonframework.queryhandling.GenericQueryMessage;
import org.axonframework.queryhandling.NoHandlerForQueryException;
import org.axonframework.queryhandling.QueryBus;
import org.axonframework.queryhandling.QueryBusSpanFactory;
import org.axonframework.queryhandling.QueryExecutionException;
import org.axonframework.queryhandling.QueryMessage;
import org.axonframework.queryhandling.QueryResponseMessage;
import org.axonframework.queryhandling.QueryUpdateEmitter;
import org.axonframework.queryhandling.StreamingQueryMessage;
import org.axonframework.queryhandling.SubscriptionQueryBackpressure;
import org.axonframework.queryhandling.SubscriptionQueryMessage;
import org.axonframework.queryhandling.SubscriptionQueryResult;
import org.axonframework.queryhandling.SubscriptionQueryUpdateMessage;
import org.axonframework.tracing.Span;
import org.axonframework.tracing.SpanScope;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * Bus de requêtes distribué par gRPC ({@code docs/backend/axon.md#bus-grpc-entre-pods}).
 *
 * <ul>
 *     <li>{@code query} : segment local si le pod a le handler, sinon un membre capable choisi en round-robin ;</li>
 *     <li>{@code scatterGather} : segment local et un appel par membre capable, bornés par le délai ;</li>
 *     <li>{@code streamingQuery} : segment local, sinon un membre capable, au rythme de l'abonné ;</li>
 *     <li>{@code subscriptionQuery} : un flux par membre capable, fusionnés ({@link DistributedSubscription}), puis
 *     résultat initial par une requête ordinaire une fois l'abonnement ouvert.</li>
 * </ul>
 *
 * <p>Chaque appel distant porte un {@code TIMEOUT} et une deadline gRPC : un pair figé fait échouer la requête dans
 * le délai, sans thread bloqué. Le contexte de trace voyage dans les métadonnées du message.</p>
 */
public class GrpcDistributedQueryBus implements QueryBus {

    private static final Logger logger = LoggerFactory.getLogger(GrpcDistributedQueryBus.class);
    private static final int DIRECT = 1;
    private static final int STREAMING = -1;
    private static final int DEFAULT_PRIORITY = 0;

    private final QueryBus localSegment;
    private final Membership membership;
    private final PeerChannels channels;
    private final QueryCodec codec;
    private final QueryBusSpanFactory spanFactory;
    private final Duration queryTimeout;
    private final QueryBusMetrics metrics;
    private final Map<String, AtomicInteger> localHandlers = new ConcurrentHashMap<>();
    private final Set<DistributedSubscription<?>> subscriptions = ConcurrentHashMap.newKeySet();
    private final AtomicInteger roundRobin = new AtomicInteger();

    public GrpcDistributedQueryBus(QueryBus localSegment,
                                   Membership membership,
                                   PeerChannels channels,
                                   QueryCodec codec,
                                   QueryBusSpanFactory spanFactory,
                                   Duration queryTimeout,
                                   QueryBusMetrics metrics) {
        this.localSegment = localSegment;
        this.membership = membership;
        this.channels = channels;
        this.codec = codec;
        this.spanFactory = spanFactory;
        this.queryTimeout = queryTimeout;
        this.metrics = metrics;
        membership.addListener(topology -> subscriptions.forEach(subscription -> subscription.reconcile(topology)));
    }

    /** Requêtes servies par le pod, annoncées à ses pairs. */
    public Set<String> localQueryNames() {
        return Set.copyOf(localHandlers.keySet());
    }

    /** Abonnements ouverts par ce pod. */
    public int activeSubscriptions() {
        return subscriptions.size();
    }

    @Override
    public <R> Registration subscribe(String queryName, Type responseType,
                                      MessageHandler<? super QueryMessage<?, R>> handler) {
        localHandlers.computeIfAbsent(queryName, name -> new AtomicInteger()).incrementAndGet();
        Registration registration = localSegment.subscribe(queryName, responseType, handler);
        return () -> {
            localHandlers.computeIfPresent(queryName,
                    (name, count) -> count.decrementAndGet() <= 0 ? null : count);
            return registration.cancel();
        };
    }

    @Override
    public <Q, R> CompletableFuture<QueryResponseMessage<R>> query(QueryMessage<Q, R> query) {
        if (handledLocally(query.getQueryName())) {
            return localSegment.query(query);
        }
        Span span = spanFactory.createQuerySpan(query, true).start();
        try (SpanScope ignored = span.makeCurrent()) {
            QueryMessage<Q, R> propagated = spanFactory.propagateContext(query);
            Optional<Member> target = pick(query.getQueryName());
            if (target.isEmpty()) {
                NoHandlerForQueryException failure = new NoHandlerForQueryException(query);
                span.recordException(failure).end();
                return CompletableFuture.failedFuture(failure);
            }
            return remoteQuery(target.get(), propagated, queryTimeout).whenComplete((response, failure) -> {
                if (failure != null) {
                    span.recordException(failure);
                } else if (response.isExceptional()) {
                    span.recordException(response.exceptionResult());
                }
                span.end();
            });
        }
    }

    @Override
    public <Q, R> Stream<QueryResponseMessage<R>> scatterGather(QueryMessage<Q, R> query, long timeout,
                                                                TimeUnit unit) {
        Duration budget = Duration.ofMillis(unit.toMillis(timeout));
        long deadline = System.nanoTime() + budget.toNanos();
        QueryMessage<Q, R> propagated = spanFactory.propagateContext(query);
        List<CompletableFuture<QueryResponseMessage<R>>> remote = remoteHandlers(query.getQueryName()).stream()
                .map(member -> remoteQuery(member, propagated, budget))
                .toList();
        Stream<QueryResponseMessage<R>> local = handledLocally(query.getQueryName())
                ? localSegment.scatterGather(query, timeout, unit)
                : Stream.empty();
        Stream<QueryResponseMessage<R>> fromPeers = remote.stream()
                .map(future -> awaitUntil(future, deadline, query.getQueryName()))
                .filter(Objects::nonNull)
                .filter(response -> !response.isExceptional());
        return Stream.concat(local, fromPeers);
    }

    @Override
    public <Q, R> Publisher<QueryResponseMessage<R>> streamingQuery(StreamingQueryMessage<Q, R> query) {
        if (handledLocally(query.getQueryName())) {
            return localSegment.streamingQuery(query);
        }
        return Flux.defer(() -> {
            Optional<Member> target = pick(query.getQueryName());
            if (target.isEmpty()) {
                return Flux.error(new NoHandlerForQueryException(query));
            }
            return remoteStream(target.get(), spanFactory.propagateContext(query));
        });
    }

    @Override
    public <Q, I, U> SubscriptionQueryResult<QueryResponseMessage<I>, SubscriptionQueryUpdateMessage<U>> subscriptionQuery(
            SubscriptionQueryMessage<Q, I, U> query, int updateBufferSize) {
        SubscriptionQueryMessage<Q, I, U> propagated = spanFactory.propagateContext(query);
        DistributedSubscription<U> subscription = new DistributedSubscription<>(propagated, localSegment,
                handledLocally(query.getQueryName()), channels, codec, updateBufferSize, queryTimeout,
                subscriptions::remove);
        subscriptions.add(subscription);
        subscription.open(membership.topology());
        Flux<SubscriptionQueryUpdateMessage<U>> updates = subscription.updates()
                .doOnNext(update -> metrics.updateReceived(query.getQueryName()));
        Mono<QueryResponseMessage<I>> initialResult = Mono.fromFuture(() -> subscription.opened()
                .thenCompose(opened -> query(initialQueryOf(propagated)))
                .thenCompose(response -> response.isExceptional()
                        ? CompletableFuture.failedFuture(response.exceptionResult())
                        : CompletableFuture.completedFuture(response)));
        return new DefaultSubscriptionQueryResult<>(initialResult, updates, () -> {
            subscription.cancel();
            return true;
        });
    }

    @Override
    @SuppressWarnings("deprecation")
    public <Q, I, U> SubscriptionQueryResult<QueryResponseMessage<I>, SubscriptionQueryUpdateMessage<U>> subscriptionQuery(
            SubscriptionQueryMessage<Q, I, U> query, SubscriptionQueryBackpressure backpressure, int updateBufferSize) {
        return subscriptionQuery(query, updateBufferSize);
    }

    /** L'émetteur local du pod : celui des projections, dont les updates partent vers les abonnés de tout le bus. */
    @Override
    public QueryUpdateEmitter queryUpdateEmitter() {
        return localSegment.queryUpdateEmitter();
    }

    @Override
    public Registration registerDispatchInterceptor(
            MessageDispatchInterceptor<? super QueryMessage<?, ?>> dispatchInterceptor) {
        return localSegment.registerDispatchInterceptor(dispatchInterceptor);
    }

    @Override
    public Registration registerHandlerInterceptor(
            MessageHandlerInterceptor<? super QueryMessage<?, ?>> handlerInterceptor) {
        return localSegment.registerHandlerInterceptor(handlerInterceptor);
    }

    private boolean handledLocally(String queryName) {
        return localHandlers.containsKey(queryName);
    }

    private List<Member> remoteHandlers(String queryName) {
        return membership.topology().queryHandlers(queryName).stream()
                .filter(member -> !member.local())
                .toList();
    }

    private Optional<Member> pick(String queryName) {
        List<Member> capable = remoteHandlers(queryName);
        if (capable.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(capable.get(Math.floorMod(roundRobin.getAndIncrement(), capable.size())));
    }

    private <Q, R> CompletableFuture<QueryResponseMessage<R>> remoteQuery(Member member, QueryMessage<Q, R> query,
                                                                          Duration timeout) {
        CompletableFuture<QueryResponseMessage<R>> result = new CompletableFuture<>();
        QueryRequest request;
        try {
            request = codec.serializeRequest(query, DIRECT, timeout.toMillis(), DEFAULT_PRIORITY);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        long start = System.nanoTime();
        QueryServiceGrpc.newStub(channels.channel(member.endpoint()))
                .withDeadlineAfter(timeout.toMillis(), TimeUnit.MILLISECONDS)
                .query(request, new StreamObserver<>() {
                    @Override
                    public void onNext(QueryResponse response) {
                        QueryResponseMessage<R> message = codec.deserializeResponse(response, query.getResponseType());
                        record(query, message.isExceptional() ? QueryBusMetrics.Outcome.ERROR
                                : QueryBusMetrics.Outcome.SUCCESS, start);
                        result.complete(message);
                    }

                    @Override
                    public void onError(Throwable failure) {
                        RuntimeException mapped = transportFailure(query, member, timeout, failure);
                        record(query, mapped.getCause() instanceof TimeoutException
                                ? QueryBusMetrics.Outcome.TIMEOUT : QueryBusMetrics.Outcome.ERROR, start);
                        result.completeExceptionally(mapped);
                    }

                    @Override
                    public void onCompleted() {
                        if (!result.isDone()) {
                            record(query, QueryBusMetrics.Outcome.ERROR, start);
                            result.completeExceptionally(new QueryExecutionException(
                                    "No response from " + member.clientId() + " for " + query.getQueryName(), null));
                        }
                    }
                });
        return result;
    }

    private <Q, R> Flux<QueryResponseMessage<R>> remoteStream(Member member, StreamingQueryMessage<Q, R> query) {
        @SuppressWarnings("unchecked")
        ResponseType<R> elementType = ResponseTypes.instanceOf(
                (Class<R>) query.getResponseType().getExpectedResponseType());
        return Flux.create(sink -> {
            QueryRequest request = codec.serializeRequest(query, STREAMING, queryTimeout.toMillis(),
                    DEFAULT_PRIORITY);
            AtomicReference<ClientCallStreamObserver<QueryRequest>> started = new AtomicReference<>();
            QueryServiceGrpc.newStub(channels.channel(member.endpoint()))
                    .withDeadlineAfter(queryTimeout.toMillis(), TimeUnit.MILLISECONDS)
                    .query(request, new ClientResponseObserver<QueryRequest, QueryResponse>() {
                        @Override
                        public void beforeStart(ClientCallStreamObserver<QueryRequest> call) {
                            call.disableAutoRequestWithInitial(0);
                            started.set(call);
                        }

                        @Override
                        public void onNext(QueryResponse response) {
                            QueryResponseMessage<R> message = codec.deserializeResponse(response, elementType);
                            if (message.isExceptional()) {
                                sink.error(message.exceptionResult());
                            } else {
                                sink.next(message);
                            }
                        }

                        @Override
                        public void onError(Throwable failure) {
                            sink.error(transportFailure(query, member, queryTimeout, failure));
                        }

                        @Override
                        public void onCompleted() {
                            sink.complete();
                        }
                    });
            // La demande n'est relayée qu'une fois l'appel démarré : request() est refusé dans beforeStart.
            ClientCallStreamObserver<QueryRequest> call = started.get();
            sink.onRequest(n -> call.request((int) Math.min(n, Integer.MAX_VALUE)));
            sink.onCancel(() -> call.cancel("cancelled by the subscriber", null));
        }, FluxSink.OverflowStrategy.BUFFER);
    }

    private static <Q, I> QueryMessage<Q, I> initialQueryOf(SubscriptionQueryMessage<Q, I, ?> query) {
        return new GenericQueryMessage<>(new GenericMessage<>(query.getPayload(), query.getMetaData()),
                query.getQueryName(), query.getResponseType());
    }

    private static RuntimeException transportFailure(QueryMessage<?, ?> query, Member member, Duration timeout,
                                                     Throwable failure) {
        if (failure instanceof StatusRuntimeException status
                && status.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
            return new QueryExecutionException("Query " + query.getQueryName() + " sent to " + member.clientId()
                    + " timed out after " + timeout, new TimeoutException(status.getMessage()));
        }
        return new QueryExecutionException("Query " + query.getQueryName() + " could not be delivered to "
                + member.clientId() + ": " + failure.getMessage(), failure);
    }

    private static <R> QueryResponseMessage<R> awaitUntil(CompletableFuture<QueryResponseMessage<R>> future,
                                                          long deadline, String queryName) {
        try {
            return future.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException | TimeoutException e) {
            logger.debug("scatterGather {}: a member did not answer in time ({})", queryName, e.getMessage());
            return null;
        }
    }

    private void record(QueryMessage<?, ?> query, QueryBusMetrics.Outcome outcome, long start) {
        metrics.queryCompleted(query.getQueryName(), outcome, Duration.ofNanos(System.nanoTime() - start));
    }
}
