package com.healthcare.rxvigilance.pipeline.source;

import com.healthcare.rxvigilance.config.KafkaConnectionConfig;
import com.healthcare.rxvigilance.domain.AlertLeadTimeUpdate;
import com.healthcare.rxvigilance.serialization.util.KafkaSourceResult;
import com.healthcare.rxvigilance.serialization.decode.KafkaTypedSourceBuilder;
import com.healthcare.rxvigilance.serialization.decode.decoders.AlertLeadTimeMapper;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

public class AlertLeadTimeKafkaSource {
    public static final OutputTag<KafkaSourceResult<AlertLeadTimeUpdate>> DEAD_LETTER_TAG =
            new OutputTag<>("alert-lead-time-ref-dead-letter") {
            };

    private AlertLeadTimeKafkaSource() {
    }

    public static DataStream<AlertLeadTimeUpdate> build(StreamExecutionEnvironment env,
                                                        KafkaConnectionConfig kafkaConfig,
                                                        ParameterTool params) {
        return KafkaTypedSourceBuilder.forType(AlertLeadTimeUpdate.class)
                .connection(kafkaConfig)
                .params(params)
                .topic("kafka.topic.alert-lead-time-ref", "alert-lead-time-ref")
                .mapper(new AlertLeadTimeMapper())
                .producedType(TypeInformation.of(new TypeHint<KafkaSourceResult<AlertLeadTimeUpdate>>() {
                }))
                .deadLetterTag(DEAD_LETTER_TAG)
                .sourceName("alert-lead-time-ref")
                .build(env);
    }
}
