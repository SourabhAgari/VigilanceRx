package com.healthcare.rxvigilance.pipeline.sink;

import com.healthcare.rxvigilance.config.KafkaConnectionConfig;
import com.healthcare.rxvigilance.domain.GapRiskAlert;
import com.healthcare.rxvigilance.domain.LapsedAlert;
import com.healthcare.rxvigilance.domain.PdcSnapshot;
import com.healthcare.rxvigilance.serde.KafkaTypedSinkBuilder;
import com.healthcare.rxvigilance.serde.kryo.RecordKryoSerializer;
import com.healthcare.rxvigilance.serde.mapper.GapRiskAlertAvroSerializer;
import com.healthcare.rxvigilance.serde.mapper.LapsedAlertAvroSerializer;
import com.healthcare.rxvigilance.serde.mapper.PdcSnapshotAvroSerializer;
import com.healthcare.rxvigilance.serde.util.DeadLetterRecord;
import com.healthcare.rxvigilance.serde.util.KafkaSourceUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

    static KafkaRecordSerializationSchema<DeadLetterRecord> deadLetterRecordSerializer(String topic) {
        return KafkaRecordSerializationSchema.<DeadLetterRecord>builder()
                .setTopic(topic)
                .setValueSerializationSchema(DeadLetterRecord::rawBytes)
                .setHeaderProvider(deadLetterRecord -> new RecordHeaders()
                        .add("error-message", deadLetterRecord.errorMessage().getBytes(StandardCharsets.UTF_8)))
                .build();
    }

    public static KafkaSink<DeadLetterRecord> deadLetterSink(StreamExecutionEnvironment env,
                                                             KafkaConnectionConfig kafkaConfig,
                                                             ParameterTool params) {
        env.getConfig().registerTypeWithKryoSerializer(DeadLetterRecord.class, RecordKryoSerializer.class);

        String topic = params.get("kafka.topic.dead-letter", "dead-letter");

        KafkaRecordSerializationSchema<DeadLetterRecord> recordSerializer = deadLetterRecordSerializer(topic);

        Properties producerProperties = KafkaSourceUtil.securityProperties(kafkaConfig);

        return KafkaSink.<DeadLetterRecord>builder()
                .setBootstrapServers(kafkaConfig.brokers())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("rx-vigilance-" + topic)
                .setKafkaProducerConfig(producerProperties)
                .setRecordSerializer(recordSerializer)
                .build();
    }
}
