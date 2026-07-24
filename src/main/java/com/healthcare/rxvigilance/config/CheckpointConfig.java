package com.healthcare.rxvigilance.config;

import org.apache.flink.api.java.utils.ParameterTool;

public record CheckpointConfig(
        String checkpointDirectory,
        long intervalMs,
        long minPauseMs,
        int tolerableFailures
) {
    public CheckpointConfig {
        if (checkpointDirectory == null || checkpointDirectory.isBlank()) {
            throw new IllegalArgumentException("checkpoint.directory is required");
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
                params.getRequired("checkpoint.dir"),
                params.getLong("checkpoint.interval.ms", 30_000L),
                params.getLong("checkpoint.min.pause.ms", 10_000L),
                params.getInt("checkpoint.tolerable.failures", 3));
    }
}
