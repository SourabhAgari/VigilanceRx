package com.healthcare.rxvigilance.config;

import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link CheckpointConfig}.
 *
 * <p>Verifies validation of checkpoint directory, interval, and tolerable
 * failure settings, as well as the handling of an absent checkpoint directory
 * when configuration is loaded from {@link ParameterTool}.
 */
class CheckpointConfigTest {

    /**
     * Verifies that a blank checkpoint directory is rejected during configuration creation.
     */
    @Test
    void rejectsBlankCheckpointDirectory() {
        assertThatThrownBy(() -> new CheckpointConfig(
                "", 30_000L, 10_000L, 3
        )).isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Verifies that a non-positive checkpoint interval is rejected.
     */
    @Test
    void rejectsNonPositiveInterval() {
        assertThatThrownBy(() -> new CheckpointConfig(
                "file:///tmp/x", 0L, 10_000L, 3
        )).isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Verifies that a negative number of tolerable checkpoint failures is rejected.
     */
    @Test
    void rejectsNegativeTolerableFailures() {
        assertThatThrownBy(() -> new CheckpointConfig(
                "file:///tmp/x", 30_000L, 10_000L, -1
        )).isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Verifies that checkpoint storage can be left unspecified.
     */
    @Test
    void allowsAbsentCheckpointDirectory() {
        CheckpointConfig config = new CheckpointConfig(
                null, 30_000L, 10_000L, 3
        );

        assertThat(config.checkpointDirectory()).isNull();
    }

    /**
     * Verifies that the checkpoint directory remains unset when the corresponding
     * parameter is absent.
     */
    @Test
    void fromParamsLeavesDirectoryNullWhenAbsent() {
        CheckpointConfig config =
                CheckpointConfig.fromParams(ParameterTool.fromMap(Map.of()));

        assertThat(config.checkpointDirectory()).isNull();
    }
}