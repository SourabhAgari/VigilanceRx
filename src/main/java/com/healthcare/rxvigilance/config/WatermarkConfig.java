package com.healthcare.rxvigilance.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.time.Duration;

public record WatermarkConfig(Duration outOfOrderness, Duration idleness) {
    private static final Long DEFAULT_OUT_OF_ORDERNESS_MS = Duration.ofHours(24).toMillis();
    private static final Long DEFAULT_IDLENESS_MS = Duration.ofMinutes(5).toMillis();

    public WatermarkConfig {
        if (outOfOrderness == null || outOfOrderness.isNegative()) {
            throw new IllegalArgumentException("watermark.out.of.orderness.ms cannot be null or negative");
        }
        if (idleness == null || idleness.isNegative()) {
            throw new IllegalArgumentException("watermark.idleness.ms cannot be null or negative");
        }
    }

    public static WatermarkConfig fromParams(ParameterTool params) {
        return new WatermarkConfig(
                Duration.ofMillis(params.getLong("watermark.out.of.orderness.ms", DEFAULT_OUT_OF_ORDERNESS_MS)),
                Duration.ofMillis(params.getLong("watermark.idleness.ms", DEFAULT_IDLENESS_MS)));
    }
}
