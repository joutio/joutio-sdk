package com.joutio.axon.grpc.query;

import com.joutio.axon.grpc.PeerChannels;
import com.joutio.axon.grpc.membership.Member;
import com.joutio.axon.grpc.membership.Topology;
import io.axoniq.axonserver.grpc.query.QueryServiceGrpc;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
import io.grpc.stub.StreamObserver;
import org.axonframework.queryhandling.NoHandlerForQueryException;
import org.axonframework.queryhandling.QueryBus;
import org.axonframework.queryhandling.QueryExecutionException;
import org.axonframework.queryhandling.SubscriptionQueryMessage;
import org.axonframework.queryhandling.SubscriptionQueryResult;
import org.axonframework.queryhandling.SubscriptionQueryUpdateMessage;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Abonnement d'un dispatcher à une vue vivante : un flux {@code Subscription} par membre capable (le pod local
 * compris, par son segment local), fusionnés en un seul flux d'updates.
 *
 * <p>N'importe quelle réplique peut posséder le segment qui traite l'événement, d'où l'abonnement à toutes. Une
 * réplique ajoutée à la topologie est abonnée ; une réplique perdue est retirée, et la perte de toutes termine
 * l'abonnement en erreur (l'abonné rouvre). Un {@code complete} d'un fournisseur termine l'abonnement ; un update en
 * erreur le termine en erreur.</p>
 *
 * <p>Ouverture : chaque fournisseur acquitte son abonnement par une réponse vide, et {@link #opened()} se résout une
 * fois tous les acquittements reçus (ou les fournisseurs muets retirés après le délai) : un résultat initial demandé
 * ensuite ne manque aucun update.</p>
 *
 * <p>Contrôle de flux : chaque fournisseur reçoit {@code permits} permis à l'ouverture, puis en regagne la moitié
 * dès que l'abonné a consommé autant de ses updates.</p>
 */
final class DistributedSubscription<U> {

    private static final Logger logger = LoggerFactory.getLogger(DistributedSubscription.class);
    private static final String LOCAL = "local";

    private final SubscriptionQueryMessage<?, ?, U> query;
    private final QueryBus localSegment;
    private final boolean localCapable;
    private final PeerChannels channels;
    private final QueryCodec codec;
    private final int permits;
    private final int refill;
    private final Duration openTimeout;
    private final Consumer<DistributedSubscription<U>> onFinished;
    private final Sinks.Many<Tagged<U>> sink = Sinks.many().unicast().onBackpressureBuffer();
    private final Map<String, Source> sources = new LinkedHashMap<>();
    private final CompletableFuture<Void> opened = new CompletableFuture<>();
    private boolean finished;

    DistributedSubscription(SubscriptionQueryMessage<?, ?, U> query,
                            QueryBus localSegment,
                            boolean localCapable,
                            PeerChannels channels,
                            QueryCodec codec,
                            int permits,
                            Duration openTimeout,
                            Consumer<DistributedSubscription<U>> onFinished) {
        this.query = query;
        this.localSegment = localSegment;
        this.localCapable = localCapable;
        this.channels = channels;
        this.codec = codec;
        this.permits = Math.max(2, permits);
        this.refill = this.permits / 2;
        this.openTimeout = openTimeout;
        this.onFinished = onFinished;
    }

    String queryName() {
        return query.getQueryName();
    }

    /** Résolu quand tous les fournisseurs du départ ont acquitté (ou ont été retirés). */
    CompletableFuture<Void> opened() {
        return opened;
    }

    Flux<SubscriptionQueryUpdateMessage<U>> updates() {
        return sink.asFlux()
                .doOnNext(tagged -> tagged.consumed().run())
                .map(Tagged::update);
    }

    /** Ouvre un flux vers chaque fournisseur de la topologie de départ. */
    void open(Topology topology) {
        List<CompletableFuture<Void>> acknowledgements = new ArrayList<>();
        synchronized (this) {
            for (Source source : missingSources(topology)) {
                sources.put(source.key(), source);
                acknowledgements.add(source.acknowledged());
            }
            if (sources.isEmpty()) {
                fail(new NoHandlerForQueryException(
                        "No handler for subscription query [" + query.getQueryName() + "]"));
                return;
            }
        }
        sourcesSnapshot().forEach(Source::start);
        CompletableFuture.allOf(acknowledgements.stream()
                        .map(acknowledged -> acknowledged.exceptionally(failure -> null))
                        .toArray(CompletableFuture[]::new))
                .whenComplete((ignored, failure) -> {
                    synchronized (this) {
                        if (finished) {
                            return;
                        }
                        if (sources.isEmpty()) {
                            fail(allLost());
                        } else {
                            opened.complete(null);
                        }
                    }
                });
    }

    /** Suit la topologie : abonne les fournisseurs apparus, retire ceux qui ont disparu. */
    void reconcile(Topology topology) {
        List<Source> added;
        List<Source> removed = new ArrayList<>();
        synchronized (this) {
            if (finished) {
                return;
            }
            List<String> wanted = wantedKeys(topology);
            sources.entrySet().removeIf(entry -> {
                if (wanted.contains(entry.getKey())) {
                    return false;
                }
                removed.add(entry.getValue());
                return true;
            });
            added = missingSources(topology);
            added.forEach(source -> sources.put(source.key(), source));
        }
        removed.forEach(source -> {
            logger.info("Subscription {} ({}) drops provider {}: left the bus",
                    query.getIdentifier(), query.getQueryName(), source.key());
            source.close();
        });
        added.forEach(source -> {
            logger.info("Subscription {} ({}) adds provider {}", query.getIdentifier(), query.getQueryName(),
                    source.key());
            source.start();
        });
        synchronized (this) {
            if (!finished && sources.isEmpty()) {
                fail(allLost());
            }
        }
    }

    /** Annulation par l'abonné. */
    void cancel() {
        finish(() -> sink.tryEmitComplete());
    }

    private List<String> wantedKeys(Topology topology) {
        List<String> keys = new ArrayList<>();
        if (localCapable) {
            keys.add(LOCAL);
        }
        topology.queryHandlers(query.getQueryName()).stream()
                .filter(member -> !member.local())
                .forEach(member -> keys.add(member.endpoint().toString()));
        return keys;
    }

    private List<Source> missingSources(Topology topology) {
        List<Source> missing = new ArrayList<>();
        if (localCapable && !sources.containsKey(LOCAL)) {
            missing.add(new LocalSource());
        }
        for (Member member : topology.queryHandlers(query.getQueryName())) {
            if (!member.local() && !sources.containsKey(member.endpoint().toString())) {
                missing.add(new RemoteSource(member));
            }
        }
        return missing;
    }

    private synchronized List<Source> sourcesSnapshot() {
        return List.copyOf(sources.values());
    }

    private void emit(Source source, SubscriptionQueryUpdateMessage<U> update) {
        synchronized (sink) {
            sink.tryEmitNext(new Tagged<>(source::consumed, update));
        }
    }

    private void lost(Source source, Throwable cause) {
        synchronized (this) {
            if (finished || sources.get(source.key()) != source) {
                return;
            }
            sources.remove(source.key());
            logger.warn("Subscription {} ({}) lost provider {}: {}", query.getIdentifier(), query.getQueryName(),
                    source.key(), cause.getMessage());
        }
        source.close();
        synchronized (this) {
            if (!finished && sources.isEmpty() && opened.isDone()) {
                fail(allLost());
            }
        }
    }

    private void completedBy(Source source) {
        logger.debug("Subscription {} completed by provider {}", query.getIdentifier(), source.key());
        finish(() -> sink.tryEmitComplete());
    }

    private void fail(Throwable failure) {
        opened.completeExceptionally(failure);
        finish(() -> sink.tryEmitError(failure));
    }

    private void finish(Runnable signal) {
        List<Source> closing;
        synchronized (this) {
            if (finished) {
                return;
            }
            finished = true;
            closing = List.copyOf(sources.values());
            sources.clear();
        }
        closing.forEach(Source::close);
        synchronized (sink) {
            signal.run();
        }
        onFinished.accept(this);
    }

    private QueryExecutionException allLost() {
        return new QueryExecutionException(
                "Subscription query [" + query.getQueryName() + "] lost all its providers", null);
    }

    private record Tagged<U>(Runnable consumed, SubscriptionQueryUpdateMessage<U> update) {
    }

    /** Un fournisseur d'updates : le segment local ou un pair. */
    private abstract class Source {

        private final CompletableFuture<Void> acknowledged = new CompletableFuture<>();
        private int consumed;

        abstract String key();

        abstract void start();

        /** Accorde {@code n} permis supplémentaires au fournisseur. */
        abstract void grant(long n);

        abstract void close();

        CompletableFuture<Void> acknowledged() {
            return acknowledged;
        }

        void consumed() {
            boolean grant;
            synchronized (this) {
                consumed++;
                grant = consumed >= refill;
                if (grant) {
                    consumed = 0;
                }
            }
            if (grant) {
                grant(refill);
            }
        }
    }

    /** Segment local : l'émetteur du pod, sans saut réseau. */
    private final class LocalSource extends Source {

        private volatile SubscriptionQueryResult<?, SubscriptionQueryUpdateMessage<U>> result;
        private final BaseSubscriber<SubscriptionQueryUpdateMessage<U>> relay = new BaseSubscriber<>() {
            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(permits);
            }

            @Override
            protected void hookOnNext(SubscriptionQueryUpdateMessage<U> update) {
                emit(LocalSource.this, update);
            }

            @Override
            protected void hookOnComplete() {
                completedBy(LocalSource.this);
            }

            @Override
            protected void hookOnError(Throwable failure) {
                fail(failure);
            }
        };

        @Override
        String key() {
            return LOCAL;
        }

        @Override
        void start() {
            try {
                result = localSegment.subscriptionQuery(query, QueryGrpcService.UPDATE_BUFFER_SIZE);
            } catch (RuntimeException e) {
                acknowledged().completeExceptionally(e);
                lost(this, e);
                return;
            }
            acknowledged().complete(null);
            result.updates().subscribe(relay);
        }

        @Override
        void grant(long n) {
            relay.request(n);
        }

        @Override
        void close() {
            relay.dispose();
            SubscriptionQueryResult<?, ?> current = result;
            if (current != null) {
                current.cancel();
            }
        }
    }

    /** Pair : un appel {@code Subscription} bidirectionnel. */
    private final class RemoteSource extends Source implements StreamObserver<SubscriptionQueryResponse> {

        private final Member member;
        private StreamObserver<SubscriptionQueryRequest> requests;
        private boolean closed;

        private RemoteSource(Member member) {
            this.member = member;
        }

        @Override
        String key() {
            return member.endpoint().toString();
        }

        @Override
        void start() {
            SubscriptionQuery subscribe;
            try {
                subscribe = codec.serializeSubscription(query, permits);
            } catch (RuntimeException e) {
                fail(e);
                return;
            }
            synchronized (this) {
                requests = QueryServiceGrpc.newStub(channels.channel(member.endpoint())).subscription(this);
                requests.onNext(SubscriptionQueryRequest.newBuilder().setSubscribe(subscribe).build());
            }
            acknowledged().orTimeout(openTimeout.toMillis(), TimeUnit.MILLISECONDS)
                    .whenComplete((ignored, failure) -> {
                        if (failure != null) {
                            lost(this, failure);
                        }
                    });
        }

        @Override
        synchronized void grant(long n) {
            if (closed) {
                return;
            }
            requests.onNext(SubscriptionQueryRequest.newBuilder()
                    .setFlowControl(SubscriptionQuery.newBuilder()
                            .setSubscriptionIdentifier(query.getIdentifier())
                            .setNumberOfPermits(n))
                    .build());
        }

        @Override
        synchronized void close() {
            if (closed || requests == null) {
                closed = true;
                return;
            }
            closed = true;
            try {
                requests.onNext(SubscriptionQueryRequest.newBuilder()
                        .setUnsubscribe(SubscriptionQuery.newBuilder()
                                .setSubscriptionIdentifier(query.getIdentifier()))
                        .build());
                requests.onCompleted();
            } catch (RuntimeException e) {
                logger.debug("Unsubscribe from {} not delivered: {}", member.endpoint(), e.getMessage());
            }
        }

        @Override
        public void onNext(SubscriptionQueryResponse response) {
            switch (response.getResponseCase()) {
                case RESPONSE_NOT_SET -> acknowledged().complete(null);
                case UPDATE -> {
                    SubscriptionQueryUpdateMessage<U> update = codec.deserializeUpdate(response.getUpdate());
                    if (update.isExceptional()) {
                        fail(update.exceptionResult());
                    } else {
                        emit(this, update);
                    }
                }
                case COMPLETE -> completedBy(this);
                case COMPLETE_EXCEPTIONALLY -> {
                    Throwable cause = codec.failureOf(response.getCompleteExceptionally());
                    acknowledged().completeExceptionally(cause);
                    lost(this, cause);
                }
                default -> logger.warn("Unexpected subscription response {} from {}", response.getResponseCase(),
                        member.endpoint());
            }
        }

        @Override
        public void onError(Throwable failure) {
            synchronized (this) {
                closed = true;
            }
            acknowledged().completeExceptionally(failure);
            lost(this, failure);
        }

        @Override
        public void onCompleted() {
            synchronized (this) {
                closed = true;
            }
            acknowledged().completeExceptionally(new IllegalStateException("stream closed by provider"));
            lost(this, new IllegalStateException("stream closed by " + member.clientId()));
        }
    }
}
