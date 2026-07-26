package com.healthcare.rxvigilance.pipeline.source;

import com.healthcare.rxvigilance.config.KafkaConnectionConfig;
import com.healthcare.rxvigilance.config.WatermarkConfig;
import com.healthcare.rxvigilance.domain.Channel;
import com.healthcare.rxvigilance.domain.EventType;
import com.healthcare.rxvigilance.domain.RxFillEvent;
import com.healthcare.rxvigilance.serialization.DeserializationResult;
import com.healthcare.rxvigilance.serialization.kryo.DeserializationResultKryoSerializer;
import com.healthcare.rxvigilance.serialization.kryo.RxFillEventKryoSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.Month;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RxFillEventKafkaSourceTest {

    @Test
    void successfulResultIsCollectedToMainOutput() throws Exception {
        OneInputStreamOperatorTestHarness<DeserializationResult, RxFillEvent> harness =
                ProcessFunctionTestHarnesses.forProcessFunction(new RxFillEventKafkaSource.DeadLetterSplitFunction());
        harness.getExecutionConfig().registerTypeWithKryoSerializer(RxFillEvent.class, RxFillEventKryoSerializer.class);
        harness.getExecutionConfig().registerTypeWithKryoSerializer(DeserializationResult.class, DeserializationResultKryoSerializer.class);
        RxFillEvent event = fillEvent((LocalDate.of(2026, Month.JULY, 20)));
        harness.processElement(DeserializationResult.success(event), 0L);

        assertThat(harness.getOutput()).extracting(o -> ((StreamRecord<RxFillEvent>) o).getValue())
                .containsExactly(event);
    }

    @Test
    void failedResultIsRoutedToDeadLetterSideOutput() throws Exception {
        OneInputStreamOperatorTestHarness<DeserializationResult, RxFillEvent> harness =
                ProcessFunctionTestHarnesses.forProcessFunction(new RxFillEventKafkaSource.DeadLetterSplitFunction());
        harness.getExecutionConfig().registerTypeWithKryoSerializer(RxFillEvent.class, RxFillEventKryoSerializer.class);
        harness.getExecutionConfig().registerTypeWithKryoSerializer(DeserializationResult.class, DeserializationResultKryoSerializer.class);
        DeserializationResult failure = DeserializationResult.failure(new byte[]{1, 2, 3}, "bad magic byte");
        harness.processElement(failure, 0L);
        assertThat(harness.getOutput()).isEmpty();
        assertThat(harness.getSideOutput(RxFillEventKafkaSource.DEAD_LETTER_TAG))
                .extracting(StreamRecord::getValue)
                .containsExactly(failure);
    }

    @Test
    void buildRejectsInvalidStartingOffsetsPolicy() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaConnectionConfig kafkaConfig = new KafkaConnectionConfig(
                "localhost:9092", "http://localhost:8081", null, null, null, null);
        WatermarkConfig watermarkConfig = new WatermarkConfig(Duration.ofHours(24), Duration.ofMinutes(5));
        ParameterTool params = ParameterTool.fromMap(Map.of("kafka.starting.offsets", "bogus"));

        assertThatThrownBy(() -> RxFillEventKafkaSource.build(env, kafkaConfig, watermarkConfig, params))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("kafka.starting.offsets");
    }

    @Test
    void buildSetsUidOnEveryOperator() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaConnectionConfig kafkaConfig = new KafkaConnectionConfig(
                "localhost:9092", "http://localhost:8081", null, null, null, null);
        WatermarkConfig watermarkConfig = new WatermarkConfig(Duration.ofHours(24), Duration.ofMinutes(5));

        RxFillEventKafkaSource.build(env, kafkaConfig, watermarkConfig, ParameterTool.fromMap(Map.of()));

        List<String> uids = env.getStreamGraph().getStreamNodes().stream()
                .map(StreamNode::getTransformationUID)
                .toList();

        assertThat(uids).containsExactlyInAnyOrder(
                "rx-fill-events-source",
                "rx-fill-events-dead-letter-split",
                "rx-fill-events-watermarks");
    }

    @Test
    void securityPropertiesEmptyWhenNoSaslCredentials() {
        KafkaConnectionConfig kafkaConfig = new KafkaConnectionConfig(
                "localhost:9092", "http://localhost:8081", null, null, null, null);

        assertThat(RxFillEventKafkaSource.securityProperties(kafkaConfig)).isEmpty();
    }

    @Test
    void securityPropertiesSetWhenSaslCredentialsPresent() {
        KafkaConnectionConfig kafkaConfig = new KafkaConnectionConfig(
                "broker:9092", "http://registry", "user", "pass", "SASL_SSL", "SCRAM-SHA-256");

        Properties properties = RxFillEventKafkaSource.securityProperties(kafkaConfig);

        assertThat(properties.getProperty("security.protocol")).isEqualTo("SASL_SSL");
        assertThat(properties.getProperty("sasl.mechanism")).isEqualTo("SCRAM-SHA-256");
        assertThat(properties.getProperty("sasl.jaas.config"))
                .contains("username=\"user\"")
                .contains("password=\"pass\"");
    }


    private static RxFillEvent fillEvent(LocalDate fillDate) {
        return new RxFillEvent(
                EventType.FILL, "CLM-1", "MBR-1", "NDC-1", fillDate, 30,
                BigDecimal.valueOf(30), "PHM-1", "RX-1", 3, Channel.RETAIL, null);
    }


}
