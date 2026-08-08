package com.healthcare.rxvigilance.serialization.encode;

import com.healthcare.rxvigilance.config.KafkaConnectionConfig;
import com.healthcare.rxvigilance.serialization.kryo.RecordKryoSerializer;
import com.healthcare.rxvigilance.serialization.codec.AvroRecordEncoder;
import com.healthcare.rxvigilance.serialization.util.KafkaSourceUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;
import java.util.Properties;

public class KafkaTypedSinkBuilder<T> {
    private final Class<T> valueType;
    private KafkaConnectionConfig kafkaConfig;
    private ParameterTool params;
    private String topicParamKey;
    private String defaultTopic;
    private AvroRecordEncoder<T> valueSerializer;

    private KafkaTypedSinkBuilder(Class<T> valueType) {
        this.valueType = Objects.requireNonNull(valueType);
    }

    public static <T> KafkaTypedSinkBuilder<T> forType(Class<T> valueType) {
        return new KafkaTypedSinkBuilder<>(valueType);
    }

    public KafkaTypedSinkBuilder<T> connection(KafkaConnectionConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
        return this;
    }

    public KafkaTypedSinkBuilder<T> params(ParameterTool params) {
        this.params = params;
        return this;
    }

    public KafkaTypedSinkBuilder<T> topic(String topicParamKey, String defaultTopic) {
        this.topicParamKey = topicParamKey;
        this.defaultTopic = defaultTopic;
        return this;
    }

    public KafkaTypedSinkBuilder<T> valueSerializer(AvroRecordEncoder<T> valueSerializer) {
        this.valueSerializer = valueSerializer;
        return this;
    }

    public KafkaSink<T> build(StreamExecutionEnvironment env) {
        Objects.requireNonNull(kafkaConfig, "connection(...) must be called");
        Objects.requireNonNull(params, "params(...) must be called");
        Objects.requireNonNull(topicParamKey, "topic(...) must be called");
        Objects.requireNonNull(valueSerializer, "valueSerializer(...) must be called");

        env.getConfig().registerTypeWithKryoSerializer(valueType, RecordKryoSerializer.class);

        String topic = params.get(topicParamKey, defaultTopic);

        KafkaRecordSerializationSchema<T> recordSerializer = KafkaRecordSerializationSchema.<T>builder()
                .setTopic(topic)
                .setValueSerializationSchema(
                        new TypedAvroSerializationSchema<>(kafkaConfig.schemaRegistryUrl(), kafkaConfig.registryConfig(), valueSerializer, topic))
                .build();

        Properties producerProperties = KafkaSourceUtil.securityProperties(kafkaConfig);

        // note: this needs to be set dynamically based on checkpoint behavior
        producerProperties.setProperty("transaction.timeout.ms",
                params.get("kafka.transaction.timeout.ms", "60000"));

        return KafkaSink.<T>builder()
                .setBootstrapServers(kafkaConfig.brokers())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("rx-vigilance-" + topic)
                .setKafkaProducerConfig(producerProperties)
                .setRecordSerializer(recordSerializer)
                .build();
    }
}
