package com.healthcare.rxvigilance.config;

import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link WatermarkConfig}.
 *
 * <p>Verifies default and explicitly configured watermark durations, validation
 * of null and negative durations, and acceptance of zero-valued durations.
 */
class WatermarkConfigTest {

    /**
     * Verifies that default watermark durations are applied when parameters
     * are not configured.
     */
    @Test
    void fromParamsAppliesDefaultsWhenNotConfigured() {
        ParameterTool empty = ParameterTool.fromMap(java.util.Collections.emptyMap());

        WatermarkConfig config = WatermarkConfig.fromParams(empty);

        assertThat(config).isNotNull();
        assertThat(config.outOfOrderness()).isEqualTo(Duration.ofHours(24));
        assertThat(config.idleness()).isEqualTo(Duration.ofMinutes(5));
    }

    /**
     * Verifies that explicitly configured watermark durations are converted
     * from milliseconds into {@link Duration} values.
     */
    @Test
    void fromParamsAppliesDefaultsWhenConfigured() {
        ParameterTool params = ParameterTool.fromMap(Map.of(
                "watermark.out.of.orderness.ms", "72000000",
                "watermark.idleness.ms", "600000"
        ));

        WatermarkConfig config = WatermarkConfig.fromParams(params);

        assertThat(config).isNotNull();
        assertThat(config.outOfOrderness()).isEqualTo(Duration.ofHours(20));
        assertThat(config.idleness()).isEqualTo(Duration.ofMinutes(10));
    }

    /**
     * Verifies that a null out-of-orderness duration is rejected.
     */
    @Test
    void rejectsNullOutOfOrderness() {
        Duration idleness = Duration.ofMinutes(5);

        assertThatThrownBy(() -> new WatermarkConfig(
                null, idleness))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Verifies that a negative out-of-orderness duration is rejected.
     */
    @Test
    void rejectsNegativeOutOfOrderness() {
        Duration negativeOutOfOrderness = Duration.ofMinutes(-5);
        Duration idleness = Duration.ofMinutes(5);

        assertThatThrownBy(() -> new WatermarkConfig(
                negativeOutOfOrderness, idleness))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Verifies that a null source idleness duration is rejected.
     */
    @Test
    void rejectsNullLiveness() {
        Duration outOfOrderness = Duration.ofMinutes(5);

        assertThatThrownBy(() -> new WatermarkConfig(
                outOfOrderness, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Verifies that a negative source idleness duration is rejected.
     */
    @Test
    void rejectsNegativeIdleness() {
        Duration negIdleNess = Duration.ofMinutes(-5);
        Duration outOfOrderness = Duration.ofMinutes(5);

        assertThatThrownBy(() -> new WatermarkConfig(
                outOfOrderness, negIdleNess))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Verifies that zero-valued out-of-orderness and idleness durations are valid.
     */
    @Test
    void acceptsZeroDurationsAsValid() {
        WatermarkConfig config = new WatermarkConfig(
                Duration.ofHours(0),
                Duration.ofMinutes(0)
        );

        assertThat(config).isNotNull();
        assertThat(config.outOfOrderness()).isEqualTo(Duration.ofHours(0));
        assertThat(config.idleness()).isEqualTo(Duration.ofMinutes(0));
    }
}