package com.healthcare.rxvigilance.pipeline.source;

import com.healthcare.rxvigilance.config.KafkaConnectionConfig;
import com.healthcare.rxvigilance.domain.DrugClassRef;
import com.healthcare.rxvigilance.domain.DrugClassRefUpdate;
import com.healthcare.rxvigilance.serialization.KafkaSourceResult;
import com.healthcare.rxvigilance.serialization.KafkaTypedSourceBuilder;
import com.healthcare.rxvigilance.serialization.mapper.DrugClassRefMapper;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

public class DrugClassRefKafkaSource {
    public static final OutputTag<KafkaSourceResult<DrugClassRefUpdate>> DEAD_LETTER_TAG =
            new OutputTag<>("ndc-drug-class-ref-dead-letter") { };

    private DrugClassRefKafkaSource() {
    }

    public static DataStream<DrugClassRefUpdate> build(StreamExecutionEnvironment env,
                                                       KafkaConnectionConfig kafkaConfig,
                                                       ParameterTool params) {
        return KafkaTypedSourceBuilder.forType(DrugClassRefUpdate.class)
                .connection(kafkaConfig)
                .params(params)
                .topic("kafka.topic.ndc-drug-class-ref", "ndc-drug-class-ref")
                .mapper(new DrugClassRefMapper())
                .producedType(TypeInformation.of(new TypeHint<KafkaSourceResult<DrugClassRefUpdate>>() { }))
                .deadLetterTag(DEAD_LETTER_TAG)
                .sourceName("ndc-drug-class-ref")
                .additionalKryoTypes(DrugClassRef.class)
                .build(env);
    }
}
