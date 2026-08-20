package com.healthcare.rxvigilance.config;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;

/**
 * Configuration for Flink state management.
 *
 * <p>Defines the state time-to-live (TTL) used to control how long state
 * remains valid and provides utilities for configuring the RocksDB state backend.
 */
public record StateBackEndConfig(int ttlDays) implements Serializable {
    private static final int DEFAULT_TTL_DAYS = 400;

    /**
     * Validates the configured state TTL.
     *
     * @param ttlDays number of days for which state remains valid
     * @throws IllegalArgumentException if the TTL is less than or equal to zero
     */
    public StateBackEndConfig {
        if(ttlDays <= 0) {
            throw new IllegalArgumentException("state.ttl.days must be greater than 0");
        }
    }

    /**
     * Creates a {@code StateBackEndConfig} from the supplied Flink parameters.
     *
     * <p>The default TTL is used when the state TTL parameter is not configured.
     *
     * @param params Flink parameters containing state backend configuration
     * @return typed state backend configuration
     */
    public static StateBackEndConfig fromParams(ParameterTool params){
        return new StateBackEndConfig(params.getInt("state.ttl.days", DEFAULT_TTL_DAYS));
    }

    /**
     * Creates the Flink state TTL configuration from the configured TTL.
     *
     * <p>Configures TTL updates on state creation and writes, prevents expired
     * state from being returned, and enables RocksDB compaction-filter cleanup.
     *
     * @return configured {@link StateTtlConfig}
     */
    public StateTtlConfig toStateTtlConfig(){
        return StateTtlConfig.newBuilder(Time.days(ttlDays))
                .updateTtlOnCreateAndWrite()
                .neverReturnExpired()
                .cleanupInRocksdbCompactFilter(1000L)
                .build();
    }

    /**
     * Configures the supplied Flink execution environment to use the
     * embedded RocksDB state backend with incremental checkpointing enabled.
     *
     * @param env Flink execution environment to configure
     */
    public static void configureRocksDbBackEnd(StreamExecutionEnvironment env){
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
    }
}