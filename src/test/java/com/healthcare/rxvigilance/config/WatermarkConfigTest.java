package com.healthcare.rxvigilance.config;

import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WatermarkConfigTest {

    @Test
    void fromParamsAppliesDefaultsWhenNotConfigured() {
        ParameterTool empty = ParameterTool.fromMap(java.util.Collections.emptyMap());
        WatermarkConfig config = WatermarkConfig.fromParams(empty);
        assertThat(config).isNotNull();
        assertThat(config.outOfOrderness()).isEqualTo(Duration.ofHours(24));
        assertThat(config.idleness()).isEqualTo(Duration.ofMinutes(5));
    }

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

    @Test
    void rejectsNullOutOfOrderness() {
        Duration idleness = Duration.ofMinutes(5);
        assertThatThrownBy(() -> new WatermarkConfig(
                null, idleness))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNegativeOutOfOrderness() {
        Duration negativeOutOfOrderness = Duration.ofMinutes(-5);
        Duration idleness = Duration.ofMinutes(5);
        assertThatThrownBy(() -> new WatermarkConfig(
                negativeOutOfOrderness, idleness))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullLiveness() {
        Duration outOfOrderness = Duration.ofMinutes(5);
        assertThatThrownBy(() -> new WatermarkConfig(
                outOfOrderness, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNegativeIdleness() {
        Duration negIdleNess = Duration.ofMinutes(-5);
        Duration outOfOrderness = Duration.ofMinutes(5);
        assertThatThrownBy(() -> new WatermarkConfig(
                outOfOrderness, negIdleNess))
                .isInstanceOf(IllegalArgumentException.class);
    }

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
