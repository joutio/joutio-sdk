package com.joutio.axon.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AxonObjectMapperAutoConfigurationTest {

    enum Status { ONLINE, OFFLINE }

    record Presence(String userId, Status status) {
    }

    private static final String NEWER_VALUE = "{\"userId\":\"u1\",\"status\":\"AWAY\"}";

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(JacksonAutoConfiguration.class, AxonObjectMapperAutoConfiguration.class));

    @Test
    void axonTransport_readsAnUnknownEnumConstantAsNull() {
        runner.run(context -> {
            ObjectMapper axon = context.getBean(AxonObjectMapperAutoConfiguration.AXON_OBJECT_MAPPER, ObjectMapper.class);
            Presence presence = axon.readValue(NEWER_VALUE, Presence.class);
            assertEquals("u1", presence.userId());
            assertNull(presence.status());
        });
    }

    @Test
    void primaryObjectMapper_staysStrict() {
        runner.run(context -> {
            ObjectMapper primary = context.getBean(ObjectMapper.class);
            assertThrows(InvalidFormatException.class, () -> primary.readValue(NEWER_VALUE, Presence.class));
        });
    }
}
