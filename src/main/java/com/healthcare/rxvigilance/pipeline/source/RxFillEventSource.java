package com.healthcare.rxvigilance.pipeline.source;

import com.healthcare.rxvigilance.config.KafkaConnectionConfig;
import com.healthcare.rxvigilance.config.WatermarkConfig;
import com.healthcare.rxvigilance.domain.RxFillEvent;
import com.healthcare.rxvigilance.serialization.KafkaSourceResult;
import com.healthcare.rxvigilance.serialization.KafkaTypedSourceBuilder;
import com.healthcare.rxvigilance.serialization.mapper.RxFillEventAvroMapper;
import com.healthcare.rxvigilance.watermark.RxFillWatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

public final class RxFillEventSource {
    public static final OutputTag<KafkaSourceResult<RxFillEvent>> DEAD_LETTER_TAG =
            new OutputTag<>("rx-fill-events-dead-letter");

    private RxFillEventSource() {}

    public static DataStream<RxFillEvent> build(StreamExecutionEnvironment env,
                                                KafkaConnectionConfig kafkaConfig,
                                                WatermarkConfig watermarkConfig,
                                                ParameterTool parameterTool) {
        DataStream<RxFillEvent> events = KafkaTypedSourceBuilder
                .forType(RxFillEvent.class)
                .connection(kafkaConfig)
                .params(parameterTool)
                .topic("kafka.topic.rx-fill-events", "rx-fill-events")
                .mapper(new RxFillEventAvroMapper())
                .producedType(TypeInformation.of(new TypeHint<KafkaSourceResult<RxFillEvent>>() {}))
                .deadLetterTag(DEAD_LETTER_TAG)
                .sourceName("rx-fill-events")
                .build(env);

        return events.assignTimestampsAndWatermarks(RxFillWatermarkStrategy.create(watermarkConfig))
                .uid("rx-fill-events-watermarks");
    }
}
