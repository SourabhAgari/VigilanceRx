package com.healthcare.rxvigilance;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class SmokeJob {
    public static void main(String[] args) throws Exception {
        Schema readerSchema;
        ParameterTool params = ParameterTool.fromArgs(args);

        String brokers = params.getRequired("kafka.brokers");
        String registryUrl = params.getRequired("schema.registry.url");
        String checkpointDir = params.getRequired("checkpoint.dir");

        String saslUsername = System.getenv("KAFKA_SASL_USERNAME");
        String saslPassword = System.getenv("KAFKA_SASL_PASSWORD");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(30_000);
        env.getCheckpointConfig().setCheckpointStorage(checkpointDir);

        // Step 2 : Kafka Config
        try (InputStream avsc = SmokeJob.class.getResourceAsStream("/rx-fill-event.avsc")) {
            readerSchema = new Schema.Parser().parse(avsc);
        }

        ConfluentRegistryAvroDeserializationSchema<GenericRecord> avroDeserializer;
        if (saslUsername != null && saslPassword != null) {
            Map<String, Object> registryConfigs = new HashMap<>();
            registryConfigs.put("basic.auth.credentials.source", "USER_INFO");
            registryConfigs.put("schema.registry.basic.auth.user.info", saslUsername + ":" + saslPassword);
            avroDeserializer = ConfluentRegistryAvroDeserializationSchema.forGeneric(
                    readerSchema, registryUrl, registryConfigs);
        } else {
            avroDeserializer = ConfluentRegistryAvroDeserializationSchema.forGeneric(readerSchema, registryUrl);
        }

        KafkaSourceBuilder<GenericRecord> kafkaSourceBuilder = KafkaSource
                .<GenericRecord>builder()
                .setBootstrapServers(brokers)
                .setTopics(params.get("kafka.topic", "rx-fill-events"))
                .setGroupId(params.get("kafka.group.id", "rx-vigilance-smoke"))
                .setStartingOffsets(OffsetsInitializer.earliest());

        if (saslUsername != null && saslPassword != null) {
            kafkaSourceBuilder
                    .setProperty("security.protocol", "SASL_SSL")
                    .setProperty("sasl.mechanism", "SCRAM-SHA-256")
                    .setProperty("sasl.jaas.config",
                            "org.apache.kafka.common.security.scram.ScramLoginModule required "
                                    + "username=\"" + saslUsername + "\" password=\"" + saslPassword + "\";");
        }

        KafkaSource<GenericRecord> source = kafkaSourceBuilder
                .setValueOnlyDeserializer(avroDeserializer)
                .build();

        DataStream<GenericRecord> events = env.fromSource(
                        source, WatermarkStrategy.noWatermarks(), "rx-fill-events-source")
                .uid("rx-fill-events-source");

        events.addSink(new LoggingSink())
                .name("smoke-logging-sink")
                .uid("smoke-logging-sink");
        env.execute("smoke-job");
    }

    private static final class LoggingSink implements SinkFunction<GenericRecord> {
        private static final Logger LOG = LoggerFactory.getLogger(LoggingSink.class);

        @Override
        public void invoke(GenericRecord value, Context context) throws Exception {
            LOG.info("Smoke event recieved: type={}, ndc={}, fillDate={}",
                    value.get("eventType"), value.get("ndcCode"), value.get("fillDate"));
            LOG.debug("smoke full record: {}", value);
        }
    }
}
