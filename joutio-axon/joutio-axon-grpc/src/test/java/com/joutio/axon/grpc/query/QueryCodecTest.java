package com.joutio.axon.grpc.query;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.joutio.axon.grpc.GrpcEndpoint;
import com.joutio.axon.grpc.LocalMember;
import com.joutio.microservice.core.domain.model.event.EventEnvelope;
import com.joutio.microservice.core.infrastructure.axon.QueryResponseTypes;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import org.axonframework.messaging.responsetypes.ResponseType;
import org.axonframework.messaging.responsetypes.ResponseTypes;
import org.axonframework.queryhandling.GenericQueryMessage;
import org.axonframework.queryhandling.GenericQueryResponseMessage;
import org.axonframework.queryhandling.QueryExecutionException;
import org.axonframework.queryhandling.QueryMessage;
import org.axonframework.queryhandling.QueryResponseMessage;
import org.axonframework.serialization.SerializationException;
import org.axonframework.serialization.json.JacksonSerializer;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verrouille la forme des réponses sur le fil : un {@code Optional} reste un {@code Optional} (vide compris), une liste
 * d'{@link EventEnvelope} garde le type concret de chaque payload, et un {@code Problem} métier (détails d'une
 * {@link QueryExecutionException}) traverse intact.
 */
class QueryCodecTest {

    record Rank(String userId, int rank) {
    }

    record SampleEvent(String gameId, int score) {
    }

    record GetRank(String userId) {
    }

    private final QueryCodec codec = codec();

    static QueryCodec codec() {
        return new QueryCodec(JacksonSerializer.builder()
                .objectMapper(JsonMapper.builder()
                        .addModule(new Jdk8Module())
                        .addModule(new JavaTimeModule())
                        .addModule(new EventEnvelopeModule())
                        .build())
                .build(), new LocalMember("game-1", "joutio-game", new GrpcEndpoint("10.0.0.1", 8124)));
    }

    @Test
    void optional_value_survives_the_wire() {
        ResponseType<Optional<Rank>> type = ResponseTypes.optionalInstanceOf(Rank.class);

        Object payload = exchange(new GenericQueryResponseMessage<>(Optional.of(new Rank("u", 7))), type).getPayload();

        assertThat(payload).isEqualTo(Optional.of(new Rank("u", 7)));
    }

    @Test
    void empty_optional_survives_the_wire() {
        ResponseType<Optional<Rank>> type = ResponseTypes.optionalInstanceOf(Rank.class);

        Object payload = exchange(new GenericQueryResponseMessage<>(Optional.empty()), type).getPayload();

        assertThat(payload).isEqualTo(Optional.empty());
    }

    @Test
    void optional_response_type_keeps_its_shape_in_the_request() {
        QueryMessage<GetRank, Optional<Rank>> query =
                new GenericQueryMessage<>(new GetRank("u"), ResponseTypes.optionalInstanceOf(Rank.class));

        QueryMessage<GetRank, Object> received = codec.deserializeRequest(codec.serializeRequest(query, 1, 1000, 0));

        assertThat(received.getResponseType()).isInstanceOf(RawOptionalResponseType.class);
        assertThat(received.getPayload()).isEqualTo(new GetRank("u"));
    }

    @Test
    void event_envelope_payload_type_survives_the_wire() {
        Instant occurredAt = Instant.parse("2026-09-27T10:00:00Z");
        EventEnvelope envelope = EventEnvelope.of("game-1", 3, occurredAt, new SampleEvent("game-1", 120));

        Object payload = exchange(new GenericQueryResponseMessage<>(List.of(envelope)), envelopes()).getPayload();

        assertThat(payload).asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.LIST).singleElement()
                .isInstanceOfSatisfying(EventEnvelope.class, restored -> {
                    assertThat(restored.aggregateId()).isEqualTo("game-1");
                    assertThat(restored.sequenceNumber()).isEqualTo(3);
                    assertThat(restored.timestamp()).isEqualTo(occurredAt);
                    assertThat(restored.eventType()).isEqualTo(SampleEvent.class.getName());
                    assertThat(restored.payload()).isEqualTo(new SampleEvent("game-1", 120));
                });
    }

    @Test
    void event_envelope_without_payload_survives_the_wire() {
        EventEnvelope envelope = EventEnvelope.of("game-1", 1, Instant.now(), null);

        Object payload = exchange(new GenericQueryResponseMessage<>(List.of(envelope)), envelopes()).getPayload();

        assertThat(payload).asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.LIST).singleElement()
                .isInstanceOfSatisfying(EventEnvelope.class, restored -> {
                    assertThat(restored.eventType()).isNull();
                    assertThat(restored.payload()).isNull();
                });
    }

    @Test
    void event_envelope_payload_outside_the_allowlist_is_rejected() {
        EventEnvelope envelope = EventEnvelope.of("game-1", 1, Instant.now(), "not-a-joutio-type");

        QueryResponseMessage<List<EventEnvelope>> back =
                exchange(new GenericQueryResponseMessage<>(List.of(envelope)), envelopes());

        assertThatThrownBy(back::getPayload).isInstanceOf(SerializationException.class);
    }

    @Test
    void business_problem_details_survive_the_wire() {
        Map<String, Object> details = Map.of("type", "game/not-found", "title", "Partie introuvable",
                "detail", "g-1", "category", "BUSINESS_RESOURCE_MISSING", "context", Map.of("gameId", "g-1"));
        QueryExecutionException problem = new QueryExecutionException("Partie introuvable", null, details);

        QueryResponseMessage<Rank> back = exchange(
                GenericQueryResponseMessage.asResponseMessage(Rank.class, problem), ResponseTypes.instanceOf(Rank.class));

        assertThat(back.isExceptional()).isTrue();
        assertThat(back.exceptionResult()).isInstanceOfSatisfying(QueryExecutionException.class, received -> {
            assertThat(received.getMessage()).contains("Partie introuvable");
            assertThat(received.getDetails()).contains(details);
        });
    }

    private <R> QueryResponseMessage<R> exchange(QueryResponseMessage<?> response, ResponseType<R> type) {
        QueryResponse wire = codec.serializeResponse(response, type, "q");
        return codec.deserializeResponse(wire, type);
    }

    @SuppressWarnings("unchecked")
    private static ResponseType<List<EventEnvelope>> envelopes() {
        return (ResponseType<List<EventEnvelope>>) (ResponseType<?>)
                QueryResponseTypes.multipleInstancesOf(EventEnvelope.class);
    }
}
