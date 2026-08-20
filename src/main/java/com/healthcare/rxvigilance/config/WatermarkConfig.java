package com.healthcare.rxvigilance.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.time.Duration;

/**
 * Configuration for event-time watermark generation.
 *
 * <p>Defines the maximum allowed out-of-orderness of events and the source
 * idleness duration used by the Flink streaming job.
 */
public record WatermarkConfig(Duration outOfOrderness, Duration idleness) {
    private static final Long DEFAULT_OUT_OF_ORDERNESS_MS = Duration.ofHours(24).toMillis();
    private static final Long DEFAULT_IDLENESS_MS = Duration.ofMinutes(5).toMillis();

    /**
     * Validates the watermark configuration.
     *
     * @param outOfOrderness maximum duration by which events may arrive out of order
     * @param idleness duration after which an idle source is considered inactive
     * @throws IllegalArgumentException if either duration is null or negative
     */
    public WatermarkConfig {
        if (outOfOrderness == null || outOfOrderness.isNegative()) {
            throw new IllegalArgumentException("watermark.out.of.orderness.ms cannot be null or negative");
        }
        if (idleness == null || idleness.isNegative()) {
            throw new IllegalArgumentException("watermark.idleness.ms cannot be null or negative");
        }
    }

    /**
     * Creates a {@code WatermarkConfig} from the supplied Flink parameters.
     *
     * <p>Default values are applied when the corresponding parameters are not configured.
     *
     * @param params Flink parameters containing watermark configuration
     * @return typed watermark configuration
     */
    public static WatermarkConfig fromParams(ParameterTool params) {
        return new WatermarkConfig(
                Duration.ofMillis(params.getLong("watermark.out.of.orderness.ms", DEFAULT_OUT_OF_ORDERNESS_MS)),
                Duration.ofMillis(params.getLong("watermark.idleness.ms", DEFAULT_IDLENESS_MS)));
    }
}