package com.healthcare.rxvigilance.pipeline.sink;

import com.healthcare.rxvigilance.config.KafkaConnectionConfig;
import com.healthcare.rxvigilance.domain.GapRiskAlert;
import com.healthcare.rxvigilance.domain.LapsedAlert;
import com.healthcare.rxvigilance.domain.PdcSnapshot;
import com.healthcare.rxvigilance.serialization.encode.KafkaTypedSinkBuilder;
import com.healthcare.rxvigilance.serialization.kryo.RecordKryoSerializer;
import com.healthcare.rxvigilance.serialization.encode.encoders.GapRiskAlertAvroSerializer;
import com.healthcare.rxvigilance.serialization.encode.encoders.LapsedAlertAvroSerializer;
import com.healthcare.rxvigilance.serialization.encode.encoders.PdcSnapshotAvroSerializer;
import com.healthcare.rxvigilance.serialization.deadletter.DeadLetterRecord;
import com.healthcare.rxvigilance.serialization.util.KafkaCoordinates;
import com.healthcare.rxvigilance.serialization.util.KafkaSourceUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.nio.charset.StandardCharsets;
import java.util.Properties;


public final class AlertKafkaSinks {
    private AlertKafkaSinks() {
    }

    public static KafkaSink<GapRiskAlert> gapRiskAlertSink(StreamExecutionEnvironment env,
                                                           KafkaConnectionConfig kafkaConfig,
                                                           ParameterTool params) {
        return KafkaTypedSinkBuilder.forType(GapRiskAlert.class)
                .connection(kafkaConfig)
                .params(params)
                .topic("kafka.topic.gap-risk-alerts", "gap-risk-alerts")
                .valueSerializer(new GapRiskAlertAvroSerializer())
                .build(env);
    }

    public static KafkaSink<LapsedAlert> lapsedAlertSink(StreamExecutionEnvironment env,
                                                         KafkaConnectionConfig kafkaConfig,
                                                         ParameterTool params) {
        return KafkaTypedSinkBuilder.forType(LapsedAlert.class)
                .connection(kafkaConfig)
                .params(params)
                .topic("kafka.topic.lapsed-alerts", "lapsed-alerts")
                .valueSerializer(new LapsedAlertAvroSerializer())
                .build(env);
    }

    public static KafkaSink<PdcSnapshot> pdcSnapshotSink(StreamExecutionEnvironment env,
                                                         KafkaConnectionConfig kafkaConfig,
                                                         ParameterTool params) {
        return KafkaTypedSinkBuilder.forType(PdcSnapshot.class)
                .connection(kafkaConfig)
                .params(params)
                .topic("kafka.topic.pdc-snapshots", "pdc-snapshots")
                .valueSerializer(new PdcSnapshotAvroSerializer())
                .build(env);
    }

    /**
     * Headers rather than Avro fields: the dead-letter topic carries raw bytes with no
     * schema, so headers are additive — nothing to register, nothing to evolve, no
     * FULL_TRANSITIVE risk, and consumers ignore what they do not recognise. These are
     * what turn "a message failed" into "this exact message, at this offset". #149.
     */
    static KafkaRecordSerializationSchema<DeadLetterRecord> deadLetterRecordSerializer(String topic) {
        return KafkaRecordSerializationSchema.<DeadLetterRecord>builder()
                .setTopic(topic)
                .setValueSerializationSchema(DeadLetterRecord::rawBytes)
                .setHeaderProvider(AlertKafkaSinks::deadLetterHeaders)
                .build();
    }

    private static Headers deadLetterHeaders(DeadLetterRecord deadLetterRecord) {
        RecordHeaders headers = new RecordHeaders();
        addHeader(headers, "error-message", deadLetterRecord.errorMessage());

        // Null when a DeadLetterRecord is built outside the Kafka source path. Skipped
        // rather than written as "null", so a consumer can tell absent from unknown.
        KafkaCoordinates coordinates = deadLetterRecord.coordinates();
        if (coordinates != null) {
            addHeader(headers, "source-topic", coordinates.topic());
            addHeader(headers, "source-partition", String.valueOf(coordinates.partition()));
            addHeader(headers, "source-offset", String.valueOf(coordinates.offset()));
            addHeader(headers, "source-timestamp", String.valueOf(coordinates.timestamp()));
        }
        return headers;
    }

    /**
     * Null-guarded: the previous version called errorMessage().getBytes() directly, so a
     * null message would have thrown inside the sink — a failure while reporting a failure.
     */
    private static void addHeader(RecordHeaders headers, String key, String value) {
        if (value != null) {
            headers.add(key, value.getBytes(StandardCharsets.UTF_8));
        }
    }

    public static KafkaSink<DeadLetterRecord> deadLetterSink(StreamExecutionEnvironment env,
                                                             KafkaConnectionConfig kafkaConfig,
                                                             ParameterTool params) {
        env.getConfig().registerTypeWithKryoSerializer(DeadLetterRecord.class, RecordKryoSerializer.class);
        env.getConfig().addDefaultKryoSerializer(Record.class, RecordKryoSerializer.class);

        String topic = params.get("kafka.topic.dead-letter", "dead-letter");

        KafkaRecordSerializationSchema<DeadLetterRecord> recordSerializer = deadLetterRecordSerializer(topic);

        Properties producerProperties = KafkaSourceUtil.producerProperties(kafkaConfig, params);

        return KafkaSink.<DeadLetterRecord>builder()
                .setBootstrapServers(kafkaConfig.brokers())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("rx-vigilance-" + topic)
                .setKafkaProducerConfig(producerProperties)
                .setRecordSerializer(recordSerializer)
                .build();
    }
}
