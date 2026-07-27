package com.healthcare.rxvigilance.serialization;

import com.healthcare.rxvigilance.config.KafkaConnectionConfig;
import com.healthcare.rxvigilance.serialization.kryo.RecordKryoSerializer;
import com.healthcare.rxvigilance.serialization.mapper.AvroValueMapper;
import com.healthcare.rxvigilance.serialization.util.DeadLetterSplitFunction;
import com.healthcare.rxvigilance.serialization.util.KafkaSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public final class KafkaTypedSourceBuilder<T> {
    private final Class<T> valueType;
    private final List<Class<?>> additionalKryoTypes = new ArrayList<>();
    private KafkaConnectionConfig kafkaConfig;
    private ParameterTool params;
    private String topicParamKey;
    private String defaultTopic;
    private AvroValueMapper<T> mapper;
    private TypeInformation<KafkaSourceResult<T>> producedType;
    private OutputTag<KafkaSourceResult<T>> deadLetterTag;
    private String sourceName;

    private KafkaTypedSourceBuilder(Class<T> valueType) {
        this.valueType = valueType;
    }

    public static <T> KafkaTypedSourceBuilder<T> forType(Class<T> valueType) {
        return new KafkaTypedSourceBuilder<>(valueType);
    }

    public KafkaTypedSourceBuilder<T> connection(KafkaConnectionConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
        return this;
    }

    public KafkaTypedSourceBuilder<T> params(ParameterTool params) {
        this.params = params;
        return this;
    }

    public KafkaTypedSourceBuilder<T> topic(String topicParamKey, String defaultTopic) {
        this.topicParamKey = topicParamKey;
        this.defaultTopic = defaultTopic;
        return this;
    }

    public KafkaTypedSourceBuilder<T> mapper(AvroValueMapper<T> mapper) {
        this.mapper = mapper;
        return this;
    }

    public KafkaTypedSourceBuilder<T> producedType(TypeInformation<KafkaSourceResult<T>> producedType) {
        this.producedType = producedType;
        return this;
    }

    public KafkaTypedSourceBuilder<T> deadLetterTag(OutputTag<KafkaSourceResult<T>> deadLetterTag) {
        this.deadLetterTag = deadLetterTag;
        return this;
    }

    public KafkaTypedSourceBuilder<T> sourceName(String sourceName) {
        this.sourceName = sourceName;
        return this;
    }

    public KafkaTypedSourceBuilder<T> additionalKryoTypes(Class<?>... types) {
        this.additionalKryoTypes.addAll(Arrays.asList(types));
        return this;
    }

    public DataStream<T> build(StreamExecutionEnvironment env) {
        Objects.requireNonNull(kafkaConfig, "connection(...) must be called");
        Objects.requireNonNull(params, "params(...) must be called");
        Objects.requireNonNull(topicParamKey, "topic(...) must be called");
        Objects.requireNonNull(mapper, "mapper(...) must be called");
        Objects.requireNonNull(producedType, "producedType(...) must be called");
        Objects.requireNonNull(deadLetterTag, "deadLetterTag(...) must be called");
        Objects.requireNonNull(sourceName, "sourceName(...) must be called");

        env.getConfig().registerTypeWithKryoSerializer(valueType, RecordKryoSerializer.class);
        env.getConfig().registerTypeWithKryoSerializer(KafkaSourceResult.class, RecordKryoSerializer.class);
        for (Class<?> type : additionalKryoTypes) {
            env.getConfig().registerTypeWithKryoSerializer(type, RecordKryoSerializer.class);
        }

        KafkaSource<KafkaSourceResult<T>> source = KafkaSource.<KafkaSourceResult<T>>builder()
                .setBootstrapServers(kafkaConfig.brokers())
                .setTopics(params.get(topicParamKey, defaultTopic))
                .setGroupId(params.get("kafka.consumer.group.id", "rx-vigilance-flink"))
                .setStartingOffsets(KafkaSourceUtil.startingOffsetsInitializer(params))
                .setDeserializer(new TypedKafkaRecordDeserialisationSchema<>(
                        kafkaConfig.schemaRegistryUrl(), mapper, producedType))
                .setProperties(KafkaSourceUtil.securityProperties(kafkaConfig))
                .build();

        DataStream<KafkaSourceResult<T>> raw = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), sourceName + "-source"
        ).uid(sourceName + "-source");

        return raw.process(new DeadLetterSplitFunction<>(deadLetterTag))
                .returns(TypeInformation.of(valueType))
                .uid(sourceName + "-dead-letter-split");
    }

}
