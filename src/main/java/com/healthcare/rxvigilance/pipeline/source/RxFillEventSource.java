package com.healthcare.rxvigilance.pipeline.source;

import com.healthcare.rxvigilance.config.KafkaConnectionConfig;
import com.healthcare.rxvigilance.config.WatermarkConfig;
import com.healthcare.rxvigilance.domain.RxFillEvent;
import com.healthcare.rxvigilance.serialization.util.KafkaSourceResult;
import com.healthcare.rxvigilance.serialization.decode.KafkaTypedSourceBuilder;
import com.healthcare.rxvigilance.serialization.decode.decoders.RxFillEventAvroMapper;
import com.healthcare.rxvigilance.watermark.RxFillWatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

public final class RxFillEventSource {
    public static final OutputTag<KafkaSourceResult<RxFillEvent>> DEAD_LETTER_TAG =
            new OutputTag<>("rx-fill-events-dead-letter") { };

    private RxFillEventSource() {}

    public static RxFillEventSourceResult build(StreamExecutionEnvironment env,
                                                KafkaConnectionConfig kafkaConfig,
                                                WatermarkConfig watermarkConfig,
                                                ParameterTool parameterTool) {
        SingleOutputStreamOperator<RxFillEvent> events = KafkaTypedSourceBuilder
                .forType(RxFillEvent.class)
                .connection(kafkaConfig)
                .params(parameterTool)
                .topic("kafka.topic.rx-fill-events", "rx-fill-events")
                .mapper(new RxFillEventAvroMapper())
                .producedType(TypeInformation.of(new TypeHint<KafkaSourceResult<RxFillEvent>>() {}))
                .deadLetterTag(DEAD_LETTER_TAG)
                .sourceName("rx-fill-events")
                .build(env);

        DataStream<KafkaSourceResult<RxFillEvent>> deadLetters = events.getSideOutput(DEAD_LETTER_TAG);

        DataStream<RxFillEvent> watermarked = events
                .assignTimestampsAndWatermarks(RxFillWatermarkStrategy.create(watermarkConfig))
                .uid("rx-fill-events-watermarks");

        return new RxFillEventSourceResult(watermarked, deadLetters);
    }

    public record RxFillEventSourceResult(
            DataStream<RxFillEvent> events,
            DataStream<KafkaSourceResult<RxFillEvent>> deadLetters) {
    }
}
