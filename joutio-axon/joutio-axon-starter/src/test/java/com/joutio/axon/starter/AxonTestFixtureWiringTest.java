package com.joutio.axon.starter;

import com.joutio.axon.test.JoutioAxonMatchers;
import org.axonframework.test.aggregate.AggregateTestFixture;
import org.junit.jupiter.api.Test;

/**
 * Test de wiring 100 % in-memory du harness Axon du starter.
 * <p>
 * Vérifie que le {@code AggregateTestFixture} du module {@code joutio-axon-test} + le helper
 * {@code JoutioAxonMatchers} sont utilisables directement (sans Spring, sans Axon Server, sans
 * Postgres) : l'agrégat de test {@link TestOrder} est instancié in-memory, la commande
 * {@link TestOrderCommand.PlaceOrderCommand} est traitée, et l'événement {@link
 * TestOrderEvent.PlacedEvent} est asserté.
 * <p>
 * Ce test est le « proof of concept » du ticket S01 : il prouve que la convention
 * AggregateTestFixture + JoutioAxonMatchers fonctionne out-of-the-box.
 */
class AxonTestFixtureWiringTest {

    private final AggregateTestFixture<TestOrder> fixture = new AggregateTestFixture<>(TestOrder.class);

    @Test
    void placeOrder_appliesPlacedEvent() {
        fixture.givenNoPriorActivity()
                .when(new TestOrderCommand.PlaceOrderCommand("order-1", 3))
                .expectEventsMatching(JoutioAxonMatchers.singlePayloadMatching(
                        TestOrderEvent.PlacedEvent.class,
                        e -> ((TestOrderEvent.PlacedEvent) e).orderId().equals("order-1")
                                && ((TestOrderEvent.PlacedEvent) e).quantity() == 3));
    }
}
