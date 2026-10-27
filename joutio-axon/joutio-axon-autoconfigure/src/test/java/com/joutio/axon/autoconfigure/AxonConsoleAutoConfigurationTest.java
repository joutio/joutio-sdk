package com.joutio.axon.autoconfigure;

import com.joutio.axon.console.ConsoleAgent;
import com.joutio.axon.console.ConsoleController;
import com.joutio.axon.console.ConsoleState;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class AxonConsoleAutoConfigurationTest {

    private final WebApplicationContextRunner runner = new WebApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(AxonConsoleAutoConfiguration.class));

    @Test
    void exposesStateAndInternalApiByDefault() {
        runner.run(context -> assertThat(context)
                .hasSingleBean(ConsoleState.class)
                .hasSingleBean(ConsoleController.class)
                .doesNotHaveBean(ConsoleAgent.class));
    }

    @Test
    void enablesAgentWhenConfigured() {
        runner.withPropertyValues("joutio.axon.console.enabled=true")
                .run(context -> assertThat(context).hasSingleBean(ConsoleAgent.class));
    }

    @Test
    void disablesInternalApiWhenConfigured() {
        runner.withPropertyValues("joutio.axon.console.internal-api-enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(ConsoleController.class));
    }
}
