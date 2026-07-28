package com.healthcare.rxvigilance.serialization;

import com.healthcare.rxvigilance.config.KafkaConnectionConfig;
import com.healthcare.rxvigilance.domain.GapRiskAlert;
import com.healthcare.rxvigilance.serialization.encode.encoders.GapRiskAlertAvroSerializer;
import com.healthcare.rxvigilance.serialization.encode.KafkaTypedSinkBuilder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaTypedSinkBuilderTest {
    private static final KafkaConnectionConfig KAFKA_CONFIG = new KafkaConnectionConfig(
            "localhost:9092", "http://localhost:8081", null, null, null, null);

    @Test
    void forTypeRejectsNullValueType() {
        assertThatThrownBy(() -> KafkaTypedSinkBuilder.forType(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void buildRequiresConnection() {
        KafkaTypedSinkBuilder<GapRiskAlert> builder = KafkaTypedSinkBuilder.forType(GapRiskAlert.class)
                .params(ParameterTool.fromMap(Map.of()))
                .topic("kafka.topic.x", "x")
                .valueSerializer(new GapRiskAlertAvroSerializer());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        assertThatThrownBy(() -> builder.build(env))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("connection");
    }

    @Test
    void buildRequiresParams() {
        KafkaTypedSinkBuilder<GapRiskAlert> builder = KafkaTypedSinkBuilder.forType(GapRiskAlert.class)
                .connection(KAFKA_CONFIG)
                .topic("kafka.topic.x", "x")
                .valueSerializer(new GapRiskAlertAvroSerializer());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        assertThatThrownBy(() -> builder.build(env))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("params");
    }

    @Test
    void buildRequiresTopic() {
        KafkaTypedSinkBuilder<GapRiskAlert> builder = KafkaTypedSinkBuilder.forType(GapRiskAlert.class)
                .connection(KAFKA_CONFIG)
                .params(ParameterTool.fromMap(Map.of()))
                .valueSerializer(new GapRiskAlertAvroSerializer());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        assertThatThrownBy(() -> builder.build(env))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("topic");
    }

    @Test
    void buildRequiresValueSerializer() {
        KafkaTypedSinkBuilder<GapRiskAlert> builder = KafkaTypedSinkBuilder.forType(GapRiskAlert.class)
                .connection(KAFKA_CONFIG)
                .params(ParameterTool.fromMap(Map.of()))
                .topic("kafka.topic.x", "x");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        assertThatThrownBy(() -> builder.build(env))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("valueSerializer");
    }

    @Test
    void buildProducesNonNullConfiguredSink() {
        KafkaSink<GapRiskAlert> sink = KafkaTypedSinkBuilder.forType(GapRiskAlert.class)
                .connection(KAFKA_CONFIG)
                .params(ParameterTool.fromMap(Map.of()))
                .topic("kafka.topic.gap-risk-alerts", "gap-risk-alerts")
                .valueSerializer(new GapRiskAlertAvroSerializer())
                .build(StreamExecutionEnvironment.getExecutionEnvironment());

        assertThat(sink).isNotNull();
    }
}
