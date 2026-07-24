package com.healthcare.rxvigilance.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CheckpointConfigTest {

    @Test
    void rejectsBlankCheckpointDirectory() {
        assertThatThrownBy(() -> new CheckpointConfig(
                "", 30_000L, 10_000L, 3
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNonPositiveInterval() {
        assertThatThrownBy(() -> new CheckpointConfig("file:///tmp/x", 0L, 10_000L, 3))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNegativeTolerableFailures() {
        assertThatThrownBy(() -> new CheckpointConfig("file:///tmp/x", 30_000L, 10_000L, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
