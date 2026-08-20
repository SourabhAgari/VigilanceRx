package com.healthcare.rxvigilance.config;

import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Configuration for Flink checkpointing.
 *
 * <p>Defines checkpoint storage, checkpoint interval, minimum pause between
 * checkpoints, and the number of checkpoint failures that can be tolerated.
 */
public record CheckpointConfig(
        String checkpointDirectory,
        long intervalMs,
        long minPauseMs,
        int tolerableFailures
) {
    /**
     * Validates the checkpoint configuration.
     *
     * <p>A null checkpoint directory indicates that checkpoint storage is
     * provided by the cluster configuration. A blank directory is considered
     * invalid configuration.
     *
     * @param checkpointDirectory checkpoint storage location, or {@code null}
     *                            when managed by the cluster
     * @param intervalMs interval between checkpoints in milliseconds
     * @param minPauseMs minimum pause between completed checkpoints in milliseconds
     * @param tolerableFailures maximum number of checkpoint failures tolerated
     * @throws IllegalArgumentException if the checkpoint directory is blank,
     *                                  the interval is non-positive, or the
     *                                  tolerable failure count is non-positive
     */
    public CheckpointConfig {
        if (checkpointDirectory != null && checkpointDirectory.isBlank()) {
            throw new IllegalArgumentException("checkpoint.dir must not be blank");
        }
        if (intervalMs <= 0) {
            throw new IllegalArgumentException("checkpoint.interval must be positive");
        }
        if (tolerableFailures <= 0) {
            throw new IllegalArgumentException("checkpoint.tolerable.failures must be positive");
        }
    }

    /**
     * Creates a {@code CheckpointConfig} from the supplied Flink parameters.
     *
     * <p>Default values are applied for checkpoint interval, minimum pause,
     * and tolerable failures when the corresponding parameters are not configured.
     *
     * @param params Flink parameters containing checkpoint configuration
     * @return typed checkpoint configuration
     */
    public static CheckpointConfig fromParams(ParameterTool params) {
        return new CheckpointConfig(
                params.get("checkpoint.dir"),
                params.getLong("checkpoint.interval.ms", 30_000L),
                params.getLong("checkpoint.min.pause.ms", 10_000L),
                params.getInt("checkpoint.tolerable.failures", 3));
    }
}