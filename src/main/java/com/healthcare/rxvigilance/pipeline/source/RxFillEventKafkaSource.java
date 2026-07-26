package com.healthcare.rxvigilance.pipeline.source;

import com.healthcare.rxvigilance.config.KafkaConnectionConfig;
import com.healthcare.rxvigilance.config.WatermarkConfig;
import com.healthcare.rxvigilance.domain.RxFillEvent;
import com.healthcare.rxvigilance.serialization.DeserializationResult;
import com.healthcare.rxvigilance.serialization.RxFillEventKafkaDeserializationSchema;
import com.healthcare.rxvigilance.watermark.RxFillWatermarkStrategy;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Properties;

public class RxFillEventKafkaSource {

    public static final OutputTag<DeserializationResult> DEAD_LETTER_TAG =
            new OutputTag<>("rx-fill-events-dead-letter") {};

    private RxFillEventKafkaSource() {}

    public static DataStream<RxFillEvent> build(StreamExecutionEnvironment env,
                                                KafkaConnectionConfig kafkaConfig,
                                                WatermarkConfig watermarkConfig,
                                                ParameterTool parameters){
        KafkaSource<DeserializationResult> source = KafkaSource.<DeserializationResult>builder()
                .setBootstrapServers(kafkaConfig.brokers())
                .setTopics(parameters.get("kafka.topic.rx-fill-events","rx-fill-events"))
                .setGroupId(parameters.get("kafka.consumer.group.id","rx-vigilance-flink"))
                .setStartingOffsets(startingOffsets(parameters))
                .setDeserializer(new RxFillEventKafkaDeserializationSchema(kafkaConfig.schemaRegistryUrl()))
                .setProperties(securityProperties(kafkaConfig))
                .build();

        DataStream<DeserializationResult> raw = env.fromSource(
                source, WatermarkStrategy.noWatermarks(),"rx-fill-events-source"
        ).uid("rx-fill-events-source");

        DataStream<RxFillEvent> events = raw.process(new DeadLetterSplitFunction()).uid("rx-fill-events-dead-letter-split");
        return events
                .assignTimestampsAndWatermarks(RxFillWatermarkStrategy.create(watermarkConfig))
                .uid("rx-fill-events-watermarks");
    }

    private static OffsetsInitializer startingOffsets(ParameterTool params) {
        String policy = params.get("kafka.starting.offsets", "earliest");
        return switch (policy) {
            case "earliest" -> OffsetsInitializer.earliest();
            case "latest" -> OffsetsInitializer.latest();
            default -> throw new IllegalArgumentException(
                    "kafka.starting.offsets must be 'earliest' or 'latest', got: " + policy);
        };
    }

    private static Properties securityProperties(KafkaConnectionConfig kafkaConfig) {
        Properties properties = new Properties();
        if (kafkaConfig.hasSaslCredentials()) {
            properties.setProperty("security.protocol", kafkaConfig.securityProtocol());
            properties.setProperty("sasl.mechanism", kafkaConfig.saslMechanism());
            properties.setProperty("sasl.jaas.config",
                    "org.apache.kafka.common.security.scram.ScramLoginModule required username=\""
                            + kafkaConfig.saslUserName() + "\" password=\""
                            + kafkaConfig.saslPassword() + "\";");
        }
        return properties;
    }

    static final class DeadLetterSplitFunction extends ProcessFunction<DeserializationResult,RxFillEvent> {

        @Override
        public void processElement(DeserializationResult deserializationResult,
                                   ProcessFunction<DeserializationResult, RxFillEvent>.Context context,
                                   Collector<RxFillEvent> collector) throws Exception {
            if(deserializationResult.isSuccess()) {
                collector.collect(deserializationResult.event());
            } else {
                context.output(DEAD_LETTER_TAG,deserializationResult);
            }

        }
    }
}
