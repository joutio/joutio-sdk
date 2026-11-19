package com.joutio.axon.autoconfigure;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeadlinePropertiesTest {

    @Test
    void defaultsToFourThreads() {
        assertThat(new DeadlineProperties(null).threads()).isEqualTo(4);
    }

    @Test
    void rejectsAnEmptyPool() {
        assertThatThrownBy(() -> new DeadlineProperties(0)).isInstanceOf(IllegalArgumentException.class);
    }
}
