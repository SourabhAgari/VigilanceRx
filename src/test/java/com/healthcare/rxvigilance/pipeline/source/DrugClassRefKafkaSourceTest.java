package com.healthcare.rxvigilance.pipeline.source;

import com.healthcare.rxvigilance.config.KafkaConnectionConfig;
import com.healthcare.rxvigilance.config.WatermarkConfig;
import com.healthcare.rxvigilance.domain.DrugClassRefUpdate;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DrugClassRefKafkaSourceTest {

    WatermarkConfig watermarkConfig = new WatermarkConfig(Duration.ofHours(24), Duration.ofMinutes(5));

    @Test
    void buildSetsUidOnSourceAndDeadLetterSplitOnly() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaConnectionConfig kafkaConfig = new KafkaConnectionConfig(
                "localhost:9092", "http://localhost:8081", null, null, null, null);

        DataStream<DrugClassRefUpdate> stream = DrugClassRefKafkaSource.build(
                env, kafkaConfig, watermarkConfig, ParameterTool.fromMap(Map.of()));

        List<String> uids = env.getStreamGraph().getStreamNodes().stream()
                .map(StreamNode::getTransformationUID)
                .toList();

        assertThat(uids).containsExactlyInAnyOrder(
                "ndc-drug-class-ref-source",
                "ndc-drug-class-ref-dead-letter-split",
                "ndc-drug-class-ref-watermarks");
        assertThat(stream).isNotNull();
    }
}
