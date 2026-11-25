package com.joutio.axon.grpc.query;

import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.QueryServiceGrpc;
import io.axoniq.axonserver.grpc.query.QueryUpdate;
import io.axoniq.axonserver.grpc.query.QueryUpdateComplete;
import io.axoniq.axonserver.grpc.query.QueryUpdateCompleteExceptionally;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.axonframework.axonserver.connector.util.ProcessingInstructionHelper;
import org.axonframework.messaging.responsetypes.ResponseType;
import org.axonframework.messaging.responsetypes.ResponseTypes;
import org.axonframework.queryhandling.GenericStreamingQueryMessage;
import org.axonframework.queryhandling.QueryBus;
import org.axonframework.queryhandling.QueryBusSpanFactory;
import org.axonframework.queryhandling.QueryMessage;
import org.axonframework.queryhandling.QueryResponseMessage;
import org.axonframework.queryhandling.SubscriptionQueryMessage;
import org.axonframework.queryhandling.SubscriptionQueryResult;
import org.axonframework.queryhandling.SubscriptionQueryUpdateMessage;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Sert les requêtes de ses pairs sur le segment local du pod ({@code QueryService} du proto d'Axon Server), jamais
 * sur le bus distribué.
 *
 * <ul>
 *     <li>{@code Query} avec {@code NR_OF_RESULTS = 1} : {@code query} local, une réponse ; sinon
 *     {@code streamingQuery} local, une réponse par élément, au rythme du dispatcher (contre-pression gRPC) ;</li>
 *     <li>{@code Subscription} : abonnement local ({@code QueryUpdateEmitter} du pod), acquitté par une réponse vide
 *     avant tout update, puis updates relayés selon les permis reçus. {@code unsubscribe}, l'annulation ou la fin du
 *     flux ferment l'abonnement local.</li>
 * </ul>
 *
 * <p>Le délai d'une requête est porté par la deadline gRPC du dispatcher, posée d'après l'instruction
 * {@code TIMEOUT} : à son expiration, l'appel est annulé des deux côtés.</p>
 */
public class QueryGrpcService extends QueryServiceGrpc.QueryServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(QueryGrpcService.class);

    /** Updates gardés par le pod pour un abonné en attente de permis (comme Axon Server). */
    static final int UPDATE_BUFFER_SIZE = 1024;

    private final QueryBus localSegment;
    private final QueryCodec codec;
    private final QueryBusSpanFactory spanFactory;
    private final Set<ProvidedSubscription> subscriptions = ConcurrentHashMap.newKeySet();
    private volatile boolean leaving;

    public QueryGrpcService(QueryBus localSegment, QueryCodec codec, QueryBusSpanFactory spanFactory) {
        this.localSegment = localSegment;
        this.codec = codec;
        this.spanFactory = spanFactory;
    }

    /** Abonnements servis par ce pod. */
    public int activeSubscriptions() {
        return subscriptions.size();
    }

    /**
     * Arrêt gracieux : plus de nouvel abonnement, et chaque abonné reçoit {@code complete_exceptionally} ; son
     * dispatcher retire ce pod et garde les autres répliques.
     */
    public void leave() {
        leaving = true;
        IllegalStateException shutdown = new IllegalStateException("Query provider is shutting down");
        subscriptions.forEach(subscription -> subscription.completeExceptionally(shutdown));
    }

    @Override
    public void query(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
        ServerCallStreamObserver<QueryResponse> call = (ServerCallStreamObserver<QueryResponse>) responseObserver;
        QueryMessage<Object, Object> query;
        try {
            query = codec.deserializeRequest(request);
            query.getPayload();
        } catch (RuntimeException e) {
            logger.warn("Could not read incoming query {}: {}", request.getQuery(), e.getMessage());
            sendLast(call, codec.errorResponse(e, request.getMessageIdentifier()));
            return;
        }
        long numberOfResults = ProcessingInstructionHelper.numberOfResults(request.getProcessingInstructionsList());
        spanFactory.createQueryProcessingSpan(query).run(() -> {
            if (numberOfResults == 1) {
                directQuery(query, request.getMessageIdentifier(), call);
            } else {
                streamingQuery(query, request.getMessageIdentifier(), call);
            }
        });
    }

    private void directQuery(QueryMessage<Object, Object> query, String requestId,
                             ServerCallStreamObserver<QueryResponse> call) {
        call.setOnCancelHandler(() -> logger.debug("Query {} cancelled by its dispatcher", query.getQueryName()));
        localSegment.query(query).whenComplete((response, failure) -> {
            QueryResponse reply;
            if (failure != null) {
                reply = codec.errorResponse(unwrap(failure), requestId);
            } else {
                try {
                    reply = codec.serializeResponse(response, query.getResponseType(), requestId);
                } catch (RuntimeException e) {
                    logger.warn("Could not serialize the response of {}", query.getQueryName(), e);
                    reply = codec.errorResponse(e, requestId);
                }
            }
            sendLast(call, reply);
        });
    }

    @SuppressWarnings("unchecked")
    private void streamingQuery(QueryMessage<Object, Object> query, String requestId,
                                ServerCallStreamObserver<QueryResponse> call) {
        Class<Object> elementType = (Class<Object>) query.getResponseType().getExpectedResponseType();
        PacedResponses responses = new PacedResponses(call, requestId, ResponseTypes.instanceOf(elementType));
        call.setOnCancelHandler(responses::dispose);
        call.setOnReadyHandler(responses::onReady);
        Flux.from(localSegment.streamingQuery(
                        new GenericStreamingQueryMessage<>(query, query.getQueryName(), elementType)))
                .subscribe(responses);
    }

    @Override
    public StreamObserver<SubscriptionQueryRequest> subscription(
            StreamObserver<SubscriptionQueryResponse> responseObserver) {
        ServerCallStreamObserver<SubscriptionQueryResponse> call =
                (ServerCallStreamObserver<SubscriptionQueryResponse>) responseObserver;
        ProvidedSubscription subscription = new ProvidedSubscription(call);
        call.setOnCancelHandler(subscription::close);
        return subscription;
    }

    private static void sendLast(ServerCallStreamObserver<QueryResponse> call, QueryResponse response) {
        if (call.isCancelled()) {
            return;
        }
        try {
            call.onNext(response);
            call.onCompleted();
        } catch (RuntimeException e) {
            logger.debug("Query response not delivered: {}", e.getMessage());
        }
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    /**
     * Relaie un {@code streamingQuery} local élément par élément : l'élément suivant n'est demandé que lorsque le
     * transport peut l'envoyer. Le dispatcher règle la cadence par {@code request(n)} sur son appel.
     */
    private final class PacedResponses extends BaseSubscriber<QueryResponseMessage<Object>> {

        private final ServerCallStreamObserver<QueryResponse> call;
        private final String requestId;
        private final ResponseType<Object> elementType;
        private final AtomicBoolean waiting = new AtomicBoolean();

        private PacedResponses(ServerCallStreamObserver<QueryResponse> call, String requestId,
                               ResponseType<Object> elementType) {
            this.call = call;
            this.requestId = requestId;
            this.elementType = elementType;
        }

        @Override
        protected void hookOnSubscribe(Subscription subscription) {
            request(1);
        }

        @Override
        protected void hookOnNext(QueryResponseMessage<Object> response) {
            try {
                call.onNext(codec.serializeResponse(response, elementType, requestId));
            } catch (RuntimeException e) {
                logger.debug("Streaming query response not delivered: {}", e.getMessage());
                dispose();
                return;
            }
            if (call.isReady()) {
                request(1);
            } else {
                waiting.set(true);
                if (call.isReady()) {
                    onReady();
                }
            }
        }

        void onReady() {
            if (waiting.compareAndSet(true, false)) {
                request(1);
            }
        }

        @Override
        protected void hookOnComplete() {
            try {
                call.onCompleted();
            } catch (RuntimeException e) {
                logger.debug("Streaming query completion not delivered: {}", e.getMessage());
            }
        }

        @Override
        protected void hookOnError(Throwable failure) {
            sendLast(call, codec.errorResponse(unwrap(failure), requestId));
        }
    }

    /** Un abonnement servi : flux de requêtes du dispatcher en entrée, updates en sortie. */
    private final class ProvidedSubscription implements StreamObserver<SubscriptionQueryRequest> {

        private final ServerCallStreamObserver<SubscriptionQueryResponse> call;
        private final AtomicBoolean closed = new AtomicBoolean();
        private volatile String subscriptionId = "";
        private volatile SubscriptionQueryResult<?, SubscriptionQueryUpdateMessage<Object>> result;
        private volatile UpdateRelay relay;
        private boolean finished;

        private ProvidedSubscription(ServerCallStreamObserver<SubscriptionQueryResponse> call) {
            this.call = call;
        }

        @Override
        public void onNext(SubscriptionQueryRequest request) {
            switch (request.getRequestCase()) {
                case SUBSCRIBE -> subscribe(request);
                case FLOW_CONTROL -> {
                    UpdateRelay current = relay;
                    long permits = request.getFlowControl().getNumberOfPermits();
                    if (current != null && permits > 0) {
                        current.request(permits);
                    }
                }
                case UNSUBSCRIBE -> finish();
                default -> logger.warn("Unsupported subscription request {} ignored", request.getRequestCase());
            }
        }

        private void subscribe(SubscriptionQueryRequest request) {
            if (result != null) {
                logger.warn("Subscription {} already open, second subscribe ignored", subscriptionId);
                return;
            }
            subscriptionId = request.getSubscribe().getSubscriptionIdentifier();
            if (leaving) {
                completeExceptionally(new IllegalStateException("Query provider is shutting down"));
                return;
            }
            SubscriptionQueryMessage<Object, Object, Object> query;
            try {
                query = codec.deserializeSubscription(request.getSubscribe());
                result = localSegment.subscriptionQuery(query, UPDATE_BUFFER_SIZE);
            } catch (RuntimeException e) {
                logger.warn("Could not open subscription {}: {}", subscriptionId, e.getMessage());
                completeExceptionally(e);
                return;
            }
            subscriptions.add(this);
            send(SubscriptionQueryResponse.newBuilder()
                    .setMessageIdentifier(UUID.randomUUID().toString())
                    .setSubscriptionIdentifier(subscriptionId)
                    .build());
            relay = new UpdateRelay(request.getSubscribe().getNumberOfPermits());
            result.updates().subscribe(relay);
        }

        void completeExceptionally(Throwable failure) {
            QueryUpdateCompleteExceptionally completion = codec.completeExceptionally(failure);
            send(response().setCompleteExceptionally(completion).build());
            finish();
        }

        void sendUpdate(QueryUpdate update) {
            send(response().setUpdate(update).build());
        }

        void complete() {
            send(response().setComplete(QueryUpdateComplete.newBuilder()).build());
            finish();
        }

        private SubscriptionQueryResponse.Builder response() {
            return SubscriptionQueryResponse.newBuilder()
                    .setMessageIdentifier(UUID.randomUUID().toString())
                    .setSubscriptionIdentifier(subscriptionId);
        }

        private synchronized void send(SubscriptionQueryResponse response) {
            if (finished || call.isCancelled()) {
                return;
            }
            try {
                call.onNext(response);
            } catch (RuntimeException e) {
                logger.debug("Subscription {} response not delivered: {}", subscriptionId, e.getMessage());
                close();
            }
        }

        private synchronized void finish() {
            close();
            if (finished) {
                return;
            }
            finished = true;
            if (!call.isCancelled()) {
                try {
                    call.onCompleted();
                } catch (RuntimeException e) {
                    logger.debug("Subscription {} completion not delivered: {}", subscriptionId, e.getMessage());
                }
            }
        }

        /** Ferme l'abonnement local, une seule fois. */
        void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            subscriptions.remove(this);
            UpdateRelay current = relay;
            if (current != null) {
                current.dispose();
            }
            SubscriptionQueryResult<?, ?> local = result;
            if (local != null) {
                local.cancel();
            }
        }

        @Override
        public void onError(Throwable t) {
            logger.debug("Subscription {} stream failed: {}", subscriptionId, t.getMessage());
            close();
        }

        @Override
        public void onCompleted() {
            finish();
        }

        /** Relaie les updates locaux, dans la limite des permis accordés par le dispatcher. */
        private final class UpdateRelay extends BaseSubscriber<SubscriptionQueryUpdateMessage<Object>> {

            private final long initialPermits;

            private UpdateRelay(long initialPermits) {
                this.initialPermits = initialPermits;
            }

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                if (initialPermits > 0) {
                    request(initialPermits);
                }
            }

            @Override
            protected void hookOnNext(SubscriptionQueryUpdateMessage<Object> update) {
                QueryUpdate serialized;
                try {
                    serialized = codec.serializeUpdate(update);
                } catch (RuntimeException e) {
                    logger.warn("Could not serialize an update of subscription {}", subscriptionId, e);
                    serialized = codec.errorUpdate(e);
                }
                sendUpdate(serialized);
            }

            @Override
            protected void hookOnComplete() {
                complete();
            }

            @Override
            protected void hookOnError(Throwable failure) {
                sendUpdate(codec.errorUpdate(failure));
                complete();
            }
        }
    }
}
