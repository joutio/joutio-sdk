package com.joutio.axon.console;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class JoutioDeadLetterQueueProviderConfigurerModuleTest {

    private static final List<String> INSTANCE_SCOPED = List.of("notification-ws", "presence-notification");

    @Test
    void dlqIsEnabledByDefaultForKebabCaseGroups() {
        JoutioAxonDlqProperties properties = new JoutioAxonDlqProperties(null, null, null, null, null);

        assertThat(JoutioDeadLetterQueueProviderConfigurerModule.isDlqEnabled(
                "game-projection", properties, INSTANCE_SCOPED)).isTrue();
    }

    @Test
    void dlqIsDisabledWhenGloballyDisabled() {
        JoutioAxonDlqProperties properties = new JoutioAxonDlqProperties(false, null, null, null, null);

        assertThat(JoutioDeadLetterQueueProviderConfigurerModule.isDlqEnabled(
                "game-projection", properties, INSTANCE_SCOPED)).isFalse();
    }

    @Test
    void dlqIsDisabledForExplicitlyExcludedGroups() {
        JoutioAxonDlqProperties properties =
                new JoutioAxonDlqProperties(true, List.of("game-projection"), null, null, null);

        assertThat(JoutioDeadLetterQueueProviderConfigurerModule.isDlqEnabled(
                "game-projection", properties, INSTANCE_SCOPED)).isFalse();
        assertThat(JoutioDeadLetterQueueProviderConfigurerModule.isDlqEnabled(
                "room-projection", properties, INSTANCE_SCOPED)).isTrue();
    }

    @Test
    void dlqIsDisabledForSagaFqcnGroups() {
        JoutioAxonDlqProperties properties = new JoutioAxonDlqProperties(null, null, null, null, null);

        assertThat(JoutioDeadLetterQueueProviderConfigurerModule.isDlqEnabled(
                "com.joutio.game.GameFlowSaga", properties, INSTANCE_SCOPED)).isFalse();
    }

    @Test
    void dlqIsDisabledForInstanceScopedGroups() {
        JoutioAxonDlqProperties properties = new JoutioAxonDlqProperties(null, null, null, null, null);

        assertThat(JoutioDeadLetterQueueProviderConfigurerModule.isDlqEnabled(
                "notification-ws", properties, INSTANCE_SCOPED)).isFalse();
        assertThat(JoutioDeadLetterQueueProviderConfigurerModule.isDlqEnabled(
                "notification-ws-bff-7c9d4f", properties, INSTANCE_SCOPED)).isFalse();
        assertThat(JoutioDeadLetterQueueProviderConfigurerModule.isDlqEnabled(
                "notification-push", properties, INSTANCE_SCOPED)).isTrue();
    }

    @Test
    void dlqIsDisabledForBlankGroup() {
        JoutioAxonDlqProperties properties = new JoutioAxonDlqProperties(null, null, null, null, null);

        assertThat(JoutioDeadLetterQueueProviderConfigurerModule.isDlqEnabled(
                " ", properties, INSTANCE_SCOPED)).isFalse();
    }
}
