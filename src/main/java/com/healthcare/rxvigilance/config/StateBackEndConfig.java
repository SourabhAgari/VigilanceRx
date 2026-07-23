package com.healthcare.rxvigilance.config;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public record StateBackEndConfig(int ttlDays) {
    private static final int DEFAULT_TTL_DAYS = 400;

    public StateBackEndConfig {
        if(ttlDays <= 0) {
            throw new IllegalArgumentException("state.days must be greater than 0");
        }
    }

    public StateBackEndConfig fromParams(ParameterTool params){
        return new StateBackEndConfig(params.getInt("ttlDays", DEFAULT_TTL_DAYS));
    }

    public StateTtlConfig toStateTtlConfig(){
        return StateTtlConfig.newBuilder(Time.days(ttlDays))
                .updateTtlOnCreateAndWrite()
                .neverReturnExpired()
                .cleanupInRocksdbCompactFilter(1000L)
                .build();
    }

    public static void configureRocksDbBackEnd(StreamExecutionEnvironment env){
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
    }
}
