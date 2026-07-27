package com.healthcare.rxvigilance.serialization;

import com.healthcare.rxvigilance.config.KafkaConnectionConfig;
import com.healthcare.rxvigilance.domain.DrugClassRefUpdate;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaTypedSourceBuilderTest {
    private static final KafkaConnectionConfig KAFKA_CONFIG = new KafkaConnectionConfig(
            "localhost:9092", "http://localhost:8081", null, null, null, null);
    private static final OutputTag<KafkaSourceResult<DrugClassRefUpdate>> DEAD_LETTER_TAG =
            new OutputTag<>("test-dead-letter") {
            };
    private static final TypeInformation<KafkaSourceResult<DrugClassRefUpdate>> PRODUCED_TYPE =
            TypeInformation.of(new TypeHint<KafkaSourceResult<DrugClassRefUpdate>>() {
            });

    @Test
    void buildRequiresConnection() {
        KafkaTypedSourceBuilder<DrugClassRefUpdate> builder = KafkaTypedSourceBuilder.forType(DrugClassRefUpdate.class)
                .params(ParameterTool.fromMap(Map.of()))
                .topic("kafka.topic.x", "x")
                .mapper((key, genericRecord) -> null)
                .producedType(PRODUCED_TYPE)
                .deadLetterTag(DEAD_LETTER_TAG)
                .sourceName("x");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        assertThatThrownBy(() -> builder.build(env))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("connection");
    }

    @Test
    void buildRequiresParams() {
        KafkaTypedSourceBuilder<DrugClassRefUpdate> builder = KafkaTypedSourceBuilder.forType(DrugClassRefUpdate.class)
                .connection(KAFKA_CONFIG)
                .topic("kafka.topic.x", "x")
                .mapper((key, genericRecord) -> null)
                .producedType(PRODUCED_TYPE)
                .deadLetterTag(DEAD_LETTER_TAG)
                .sourceName("x");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        assertThatThrownBy(() -> builder.build(env))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("params");
    }

    @Test
    void buildRequiresTopic() {
        KafkaTypedSourceBuilder<DrugClassRefUpdate> builder = KafkaTypedSourceBuilder.forType(DrugClassRefUpdate.class)
                .connection(KAFKA_CONFIG)
                .params(ParameterTool.fromMap(Map.of()))
                .mapper((key, genericRecord) -> null)
                .producedType(PRODUCED_TYPE)
                .deadLetterTag(DEAD_LETTER_TAG)
                .sourceName("x");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        assertThatThrownBy(() -> builder.build(env))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("topic");
    }

    @Test
    void buildRequiresMapper() {
        KafkaTypedSourceBuilder<DrugClassRefUpdate> builder = KafkaTypedSourceBuilder.forType(DrugClassRefUpdate.class)
                .connection(KAFKA_CONFIG)
                .params(ParameterTool.fromMap(Map.of()))
                .topic("kafka.topic.x", "x")
                .producedType(PRODUCED_TYPE)
                .deadLetterTag(DEAD_LETTER_TAG)
                .sourceName("x");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        assertThatThrownBy(() -> builder.build(env))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("mapper");
    }

    @Test
    void buildRequiresProducedType() {
        KafkaTypedSourceBuilder<DrugClassRefUpdate> builder = KafkaTypedSourceBuilder.forType(DrugClassRefUpdate.class)
                .connection(KAFKA_CONFIG)
                .params(ParameterTool.fromMap(Map.of()))
                .topic("kafka.topic.x", "x")
                .mapper((key, genericRecord) -> null)
                .deadLetterTag(DEAD_LETTER_TAG)
                .sourceName("x");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        assertThatThrownBy(() -> builder.build(env))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("producedType");
    }

    @Test
    void buildRequiresDeadLetterTag() {
        KafkaTypedSourceBuilder<DrugClassRefUpdate> builder = KafkaTypedSourceBuilder.forType(DrugClassRefUpdate.class)
                .connection(KAFKA_CONFIG)
                .params(ParameterTool.fromMap(Map.of()))
                .topic("kafka.topic.x", "x")
                .mapper((key, genericRecord) -> null)
                .producedType(PRODUCED_TYPE)
                .sourceName("x");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        assertThatThrownBy(() -> builder.build(env))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("deadLetterTag");
    }

    @Test
    void buildRequiresSourceName() {
        KafkaTypedSourceBuilder<DrugClassRefUpdate> builder = KafkaTypedSourceBuilder.forType(DrugClassRefUpdate.class)
                .connection(KAFKA_CONFIG)
                .params(ParameterTool.fromMap(Map.of()))
                .topic("kafka.topic.x", "x")
                .mapper((key, genericRecord) -> null)
                .producedType(PRODUCED_TYPE)
                .deadLetterTag(DEAD_LETTER_TAG);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        assertThatThrownBy(() -> builder.build(env))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("sourceName");
    }

    @Test
    void buildRejectsInvalidStartingOffsetsPolicy() {
        ParameterTool params = ParameterTool.fromMap(Map.of("kafka.starting.offsets", "bogus"));

        KafkaTypedSourceBuilder<DrugClassRefUpdate> builder = KafkaTypedSourceBuilder.forType(DrugClassRefUpdate.class)
                .connection(KAFKA_CONFIG)
                .params(params)
                .topic("kafka.topic.x", "x")
                .mapper((key, genericRecord) -> null)
                .producedType(PRODUCED_TYPE)
                .deadLetterTag(DEAD_LETTER_TAG)
                .sourceName("x");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        assertThatThrownBy(() -> builder.build(env))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("kafka.starting.offsets");
    }

    @Test
    void buildSetsUidOnSourceAndDeadLetterSplitOperators() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<DrugClassRefUpdate> stream = KafkaTypedSourceBuilder.forType(DrugClassRefUpdate.class)
                .connection(KAFKA_CONFIG)
                .params(ParameterTool.fromMap(Map.of()))
                .topic("kafka.topic.ndc-drug-class-ref", "ndc-drug-class-ref")
                .mapper((key, genericRecord) -> null)
                .producedType(PRODUCED_TYPE)
                .deadLetterTag(DEAD_LETTER_TAG)
                .sourceName("ndc-drug-class-ref")
                .build(env);

        List<String> uids = env.getStreamGraph().getStreamNodes().stream()
                .map(StreamNode::getTransformationUID)
                .toList();

        assertThat(uids).containsExactlyInAnyOrder(
                "ndc-drug-class-ref-source",
                "ndc-drug-class-ref-dead-letter-split");
        assertThat(stream).isNotNull();
    }


}
