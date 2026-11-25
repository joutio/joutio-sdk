package com.joutio.axon.grpc.query;

import com.joutio.axon.grpc.LocalMember;
import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.QueryUpdate;
import io.axoniq.axonserver.grpc.query.QueryUpdateCompleteExceptionally;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import org.axonframework.axonserver.connector.AxonServerConfiguration;
import org.axonframework.axonserver.connector.ErrorCode;
import org.axonframework.axonserver.connector.query.QuerySerializer;
import org.axonframework.axonserver.connector.query.subscription.SubscriptionMessageSerializer;
import org.axonframework.axonserver.connector.util.ExceptionSerializer;
import org.axonframework.messaging.GenericMessage;
import org.axonframework.messaging.responsetypes.ConvertingResponseMessage;
import org.axonframework.messaging.responsetypes.MultipleInstancesResponseType;
import org.axonframework.messaging.responsetypes.ResponseType;
import org.axonframework.queryhandling.GenericQueryMessage;
import org.axonframework.queryhandling.GenericQueryResponseMessage;
import org.axonframework.queryhandling.GenericSubscriptionQueryMessage;
import org.axonframework.queryhandling.NoHandlerForQueryException;
import org.axonframework.queryhandling.QueryMessage;
import org.axonframework.queryhandling.QueryResponseMessage;
import org.axonframework.queryhandling.SubscriptionQueryMessage;
import org.axonframework.queryhandling.SubscriptionQueryUpdateMessage;
import org.axonframework.serialization.Serializer;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Optional;

/**
 * Messages du bus de requêtes ↔ messages du proto d'Axon Server.
 *
 * <p>S'appuie sur les sérialiseurs d'{@code axon-server-connector} ({@link QuerySerializer},
 * {@link SubscriptionMessageSerializer}), avec le sérialiseur Jackson du bus (codec {@link EventEnvelopeModule}).
 * Deux adaptations, reprises de l'ancien bus HTTP, gardent la forme des réponses :</p>
 * <ul>
 *     <li>un {@code ResponseType} optionnel voyage en {@link RawOptionalResponseType} : Axon le dégraderait en
 *     réponse simple ;</li>
 *     <li>la réponse voyage sans {@link Optional} (l'élément, ou rien) et une collection voyage en tableau typé :
 *     le type des éléments survit au JSON. Le dispatcher reconstruit la forme attendue par
 *     {@link ResponseType#convert(Object)}.</li>
 * </ul>
 */
public class QueryCodec {

    private final String clientId;
    private final String componentName;
    private final QuerySerializer querySerializer;
    private final SubscriptionMessageSerializer subscriptionSerializer;

    public QueryCodec(Serializer serializer, LocalMember localMember) {
        this.clientId = localMember.clientId();
        this.componentName = localMember.componentName();
        AxonServerConfiguration configuration = new AxonServerConfiguration();
        configuration.setClientId(clientId);
        configuration.setComponentName(componentName);
        this.querySerializer = new QuerySerializer(serializer, serializer, configuration);
        this.subscriptionSerializer = new SubscriptionMessageSerializer(serializer, serializer, configuration);
    }

    public QueryRequest serializeRequest(QueryMessage<?, ?> query, int numberOfResults, long timeoutMillis,
                                         int priority) {
        return querySerializer.serializeRequest(onWire(query), numberOfResults, timeoutMillis, priority);
    }

    public <Q, R> QueryMessage<Q, R> deserializeRequest(QueryRequest request) {
        return querySerializer.deserializeRequest(request);
    }

    /** Réponse du pod qui traite la requête, mise en forme de transport. */
    public QueryResponse serializeResponse(QueryResponseMessage<?> response, ResponseType<?> responseType,
                                           String requestId) {
        if (response.isExceptional()) {
            return serializeResponse(response, requestId);
        }
        Object payload = response.getPayload();
        Object transported = forTransport(payload, responseType);
        if (transported == payload) {
            return serializeResponse(response, requestId);
        }
        @SuppressWarnings({"unchecked", "rawtypes"})
        GenericMessage<?> message = new GenericMessage(response.getIdentifier(),
                transported == null ? responseType.getExpectedResponseType() : transported.getClass(),
                transported, response.getMetaData());
        return serializeResponse(new GenericQueryResponseMessage<>(message), requestId);
    }

    private QueryResponse serializeResponse(QueryResponseMessage<?> response, String requestId) {
        if (response.isExceptional() && response.exceptionResult() instanceof NoHandlerForQueryException noHandler) {
            return errorResponse(noHandler, requestId);
        }
        return querySerializer.serializeResponse(response, requestId);
    }

    /** Échec sans réponse du handler (désérialisation, délai, absence de handler). */
    public QueryResponse errorResponse(Throwable failure, String requestId) {
        return QueryResponse.newBuilder()
                .setErrorCode(errorCode(failure).errorCode())
                .setErrorMessage(error(failure))
                .setRequestIdentifier(requestId)
                .build();
    }

    /** Réponse reçue par le dispatcher, convertie vers le type attendu (y compris une erreur distante). */
    public <R> QueryResponseMessage<R> deserializeResponse(QueryResponse response, ResponseType<R> expectedType) {
        return new ConvertingResponseMessage<>(expectedType, querySerializer.deserializeResponse(response));
    }

    public SubscriptionQuery serializeSubscription(SubscriptionQueryMessage<?, ?, ?> query, long permits) {
        SubscriptionQueryMessage<?, ?, ?> wire = onWire(query);
        return SubscriptionQuery.newBuilder()
                .setSubscriptionIdentifier(query.getIdentifier())
                .setNumberOfPermits(permits)
                .setQueryRequest(subscriptionSerializer.serializeQuery(wire))
                .setUpdateResponseType(subscriptionSerializer.serializeUpdateType(wire))
                .build();
    }

    public <Q, I, U> SubscriptionQueryMessage<Q, I, U> deserializeSubscription(SubscriptionQuery subscription) {
        return subscriptionSerializer.deserialize(subscription);
    }

    public QueryUpdate serializeUpdate(SubscriptionQueryUpdateMessage<?> update) {
        return subscriptionSerializer.serialize(update);
    }

    /** Update en erreur : l'émetteur a terminé l'abonnement sur une exception. */
    public QueryUpdate errorUpdate(Throwable failure) {
        return QueryUpdate.newBuilder()
                .setErrorCode(errorCode(failure).errorCode())
                .setErrorMessage(error(failure))
                .setClientId(clientId)
                .setComponentName(componentName)
                .build();
    }

    public <U> SubscriptionQueryUpdateMessage<U> deserializeUpdate(QueryUpdate update) {
        return subscriptionSerializer.deserialize(update);
    }

    public QueryUpdateCompleteExceptionally completeExceptionally(Throwable failure) {
        return QueryUpdateCompleteExceptionally.newBuilder()
                .setErrorCode(errorCode(failure).errorCode())
                .setErrorMessage(error(failure))
                .setClientId(clientId)
                .setComponentName(componentName)
                .build();
    }

    public Throwable failureOf(QueryUpdateCompleteExceptionally completion) {
        return ErrorCode.getFromCode(completion.getErrorCode()).convert(completion.getErrorMessage());
    }

    private ErrorMessage error(Throwable failure) {
        return ExceptionSerializer.serialize(clientId, failure);
    }

    private static ErrorCode errorCode(Throwable failure) {
        return failure instanceof NoHandlerForQueryException
                ? ErrorCode.NO_HANDLER_FOR_QUERY
                : ErrorCode.getQueryExecutionErrorCode(failure);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static QueryMessage<?, ?> onWire(QueryMessage<?, ?> query) {
        ResponseType<?> wire = ResponseTypeWire.forSerialization(query.getResponseType());
        if (wire == query.getResponseType()) {
            return query;
        }
        return new GenericQueryMessage(query, query.getQueryName(), wire);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static SubscriptionQueryMessage<?, ?, ?> onWire(SubscriptionQueryMessage<?, ?, ?> query) {
        ResponseType<?> wire = ResponseTypeWire.forSerialization(query.getResponseType());
        if (wire == query.getResponseType()) {
            return query;
        }
        return new GenericSubscriptionQueryMessage(query, query.getQueryName(), wire, query.getUpdateResponseType());
    }

    static Object forTransport(Object payload, ResponseType<?> responseType) {
        if (payload instanceof Optional<?> optional) {
            return optional.orElse(null);
        }
        if (responseType instanceof MultipleInstancesResponseType<?> && payload instanceof Collection<?> collection) {
            Object array = Array.newInstance(responseType.getExpectedResponseType(), collection.size());
            int index = 0;
            for (Object element : collection) {
                Array.set(array, index++, element);
            }
            return array;
        }
        return payload;
    }
}
