package com.healthcare.rxvigilance.pipeline.sink;

import com.healthcare.rxvigilance.config.KafkaConnectionConfig;
import com.healthcare.rxvigilance.domain.GapRiskAlert;
import com.healthcare.rxvigilance.domain.LapsedAlert;
import com.healthcare.rxvigilance.domain.PdcSnapshot;
import com.healthcare.rxvigilance.serialization.deadletter.DeadLetterRecord;
import com.healthcare.rxvigilance.serialization.util.KafkaCoordinates;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AlertKafkaSinksTest {

    private static final KafkaConnectionConfig KAFKA_CONFIG = new KafkaConnectionConfig(
            "localhost:9092", "http://localhost:8081", null, null, null, null);

    @Test
    void gapRiskAlertSinkBuildsNonNullSink() {
        KafkaSink<GapRiskAlert> sink = AlertKafkaSinks.gapRiskAlertSink(
                StreamExecutionEnvironment.getExecutionEnvironment(), KAFKA_CONFIG, ParameterTool.fromMap(Map.of()));

        assertThat(sink).isNotNull();
    }

    @Test
    void lapsedAlertSinkBuildsNonNullSink() {
        KafkaSink<LapsedAlert> sink = AlertKafkaSinks.lapsedAlertSink(
                StreamExecutionEnvironment.getExecutionEnvironment(), KAFKA_CONFIG, ParameterTool.fromMap(Map.of()));

        assertThat(sink).isNotNull();
    }

    @Test
    void pdcSnapshotSinkBuildsNonNullSink() {
        KafkaSink<PdcSnapshot> sink = AlertKafkaSinks.pdcSnapshotSink(
                StreamExecutionEnvironment.getExecutionEnvironment(), KAFKA_CONFIG, ParameterTool.fromMap(Map.of()));

        assertThat(sink).isNotNull();
    }

    @Test
    void deadLetterSinkBuildsNonNullSink() {
        KafkaSink<DeadLetterRecord> sink = AlertKafkaSinks.deadLetterSink(
                StreamExecutionEnvironment.getExecutionEnvironment(), KAFKA_CONFIG, ParameterTool.fromMap(Map.of()));

        assertThat(sink).isNotNull();
    }

    @Test
    void deadLetterRecordSerializerWritesRawBytesAsValueAndErrorMessageAsHeaderAndSourceCoordinatesAsHeaders() {
        KafkaRecordSerializationSchema<DeadLetterRecord> schema =
                AlertKafkaSinks.deadLetterRecordSerializer("dead-letter");

        DeadLetterRecord deadLetterRecord = new DeadLetterRecord(new byte[]{1, 2, 3},
                "bad magic byte",
                new KafkaCoordinates("rx-fill-events", 2, 884211L, 1_700_000_000_000L));

        ProducerRecord<byte[], byte[]> producerRecord = schema.serialize(deadLetterRecord, new NoOpSinkContext(), null);

        assertThat(producerRecord.topic()).isEqualTo("dead-letter");
        assertThat(producerRecord.value()).isEqualTo(new byte[]{1, 2, 3});
        assertThat(producerRecord.headers().lastHeader("error-message").value())
                .isEqualTo("bad magic byte".getBytes(StandardCharsets.UTF_8));
        assertThat(producerRecord.headers().lastHeader("source-topic").value())
                .isEqualTo("rx-fill-events".getBytes(StandardCharsets.UTF_8));
        assertThat(producerRecord.headers().lastHeader("source-partition").value())
                .isEqualTo("2".getBytes(StandardCharsets.UTF_8));
        assertThat(producerRecord.headers().lastHeader("source-offset").value())
                .isEqualTo("884211".getBytes(StandardCharsets.UTF_8));
    }

    private static final class NoOpSinkContext implements KafkaRecordSerializationSchema.KafkaSinkContext {
        @Override
        public int getParallelInstanceId() {
            return 0;
        }

        @Override
        public int getNumberOfParallelInstances() {
            return 1;
        }

        @Override
        public int[] getPartitionsForTopic(String topic) {
            return new int[0];
        }
    }

    @Test
    void deadLetterRecordSerializerOmitsCoordinateHeadersWhenAbsent() {
        KafkaRecordSerializationSchema<DeadLetterRecord> schema =
                AlertKafkaSinks.deadLetterRecordSerializer("dead-letter");

        DeadLetterRecord noCoordinates =
                new DeadLetterRecord(new byte[]{1, 2, 3}, "bad magic byte", null);

        ProducerRecord<byte[], byte[]> producerRecord =
                schema.serialize(noCoordinates, new NoOpSinkContext(), null);

        assertThat(producerRecord.headers().lastHeader("error-message")).isNotNull();
        assertThat(producerRecord.headers().lastHeader("source-topic")).isNull();
    }
}
