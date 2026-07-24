package com.healthcare.rxvigilance.config;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StateBackEndConfigTest {

    @Test
    void rejectsNonPositiveTtlDays() {
        assertThatThrownBy(() -> new StateBackEndConfig(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void toStateTtlConfigUsesConfiguredDays() {
        StateBackEndConfig config = new StateBackEndConfig(400);

        StateTtlConfig ttlConfig = config.toStateTtlConfig();

        assertThat(ttlConfig.getTtl()).isEqualTo(Time.days(400));
        assertThat(ttlConfig.getUpdateType()).isEqualTo(StateTtlConfig.UpdateType.OnCreateAndWrite);
        assertThat(ttlConfig.getStateVisibility()).isEqualTo(StateTtlConfig.StateVisibility.NeverReturnExpired);
    }

    @Test
    void configureRocksDbBackEndSetsRocksDbAsStateBackend() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StateBackEndConfig.configureRocksDbBackEnd(env);

        assertThat(env.getStateBackend()).isInstanceOf(EmbeddedRocksDBStateBackend.class);
    }

}
