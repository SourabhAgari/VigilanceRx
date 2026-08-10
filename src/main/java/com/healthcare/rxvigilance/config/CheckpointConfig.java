package com.healthcare.rxvigilance.config;

import org.apache.flink.api.java.utils.ParameterTool;

public record CheckpointConfig(
        String checkpointDirectory,
        long intervalMs,
        long minPauseMs,
        int tolerableFailures
) {
    public CheckpointConfig {
        // null means "not configured here": the job leaves checkpoint storage to
        // the cluster configuration, which is how it runs under the Flink
        // operator (state.checkpoints.dir in flink-deployment.yaml). A blank
        // string is still a typo, not an intention.
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

    public static CheckpointConfig fromParams(ParameterTool params) {
        return new CheckpointConfig(
                params.get("checkpoint.dir"),
                params.getLong("checkpoint.interval.ms", 30_000L),
                params.getLong("checkpoint.min.pause.ms", 10_000L),
                params.getInt("checkpoint.tolerable.failures", 3));
    }
}
