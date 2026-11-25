package com.joutio.axon.autoconfigure;

import org.axonframework.config.Configuration;
import org.axonframework.config.Configurer;
import org.axonframework.config.DefaultConfigurer;
import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.eventhandling.SubscribingEventProcessor;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventhandling.TrackingEventProcessor;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.StreamableMessageSource;
import org.axonframework.messaging.unitofwork.DefaultUnitOfWork;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class LocalProcessingGroupsAutoConfigurationTest {

    private static final List<String> RECEIVED = new ArrayList<>();

    @Test
    void localGroup_isASubscribingProcessorOnTheEventBusOfThePod() {
        RECEIVED.clear();
        Configuration configuration = configure(List.of("live"));
        configuration.start();
        try {
            assertThat(configuration.eventProcessingConfiguration().eventProcessors().get("live"))
                    .isInstanceOf(SubscribingEventProcessor.class);

            configuration.eventBus().publish(GenericEventMessage.asEventMessage(new LiveEvent("e1")));

            assertThat(RECEIVED).containsExactly("e1");
        } finally {
            configuration.shutdown();
        }
    }

    @Test
    void aFailingLocalHandler_neverRollsBackThePublishingUnitOfWork() {
        RECEIVED.clear();
        Configuration configuration = configure(List.of("live"));
        configuration.start();
        try {
            DefaultUnitOfWork<?> unitOfWork = DefaultUnitOfWork.startAndGet(null);
            unitOfWork.execute(() -> configuration.eventBus()
                    .publish(GenericEventMessage.asEventMessage(new LiveEvent("boom"))));

            assertThat(unitOfWork.isRolledBack()).isFalse();
            assertThat(RECEIVED).containsExactly("boom");
        } finally {
            configuration.shutdown();
        }
    }

    @Test
    void withoutLocalGroups_theAnnotatedGroupStaysATrackingProcessor() {
        Configuration configuration = configure(List.of());

        assertThat(configuration.eventProcessingConfiguration().eventProcessors().get("live"))
                .isInstanceOf(TrackingEventProcessor.class);
    }

    private static Configuration configure(List<String> groups) {
        Configurer configurer = DefaultConfigurer.defaultConfiguration()
                .configureEmbeddedEventStore(configuration -> new InMemoryEventStorageEngine());
        configurer.eventProcessing()
                .configureDefaultStreamableMessageSource(configuration -> mockStreamableSource())
                .registerEventHandler(configuration -> new LiveHandler());

        new LocalProcessingGroupsAutoConfiguration()
                .localProcessingGroupsConfigurer(new LocalProcessingGroupsProperties(groups))
                .configureModule(configurer);

        return configurer.buildConfiguration();
    }

    @SuppressWarnings("unchecked")
    private static StreamableMessageSource<TrackedEventMessage<?>> mockStreamableSource() {
        return mock(StreamableMessageSource.class);
    }

    @ProcessingGroup("live")
    static class LiveHandler {

        @EventHandler
        public void on(LiveEvent event) {
            RECEIVED.add(event.id());
            if (event.id().equals("boom")) {
                throw new IllegalStateException("boom");
            }
        }
    }

    record LiveEvent(String id) {
    }
}
