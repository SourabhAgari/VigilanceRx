package com.healthcare.rxvigilance.integration;

import com.healthcare.rxvigilance.config.KafkaConnectionConfig;
import com.healthcare.rxvigilance.config.WatermarkConfig;
import com.healthcare.rxvigilance.domain.DrugClassRef;
import com.healthcare.rxvigilance.domain.DrugClassRefUpdate;
import com.healthcare.rxvigilance.domain.RxFillEvent;
import com.healthcare.rxvigilance.domain.enums.Channel;
import com.healthcare.rxvigilance.domain.enums.EventType;
import com.healthcare.rxvigilance.pipeline.source.DrugClassRefKafkaSource;
import com.healthcare.rxvigilance.pipeline.source.RxFillEventSource;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.InputStream;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.Month;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class KafkaSourceSinkRoundTripIT {

    @Container
    static final RedpandaContainer RED_PANDA_CONTAINER =
            new RedpandaContainer(DockerImageName.parse("redpandadata/redpanda:v24.1.7"));

    static MiniClusterWithClientResource flinkCluster;

    @BeforeAll
    static void startFlinkCluster() throws Exception {
        flinkCluster = new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setNumberTaskManagers(1)
                        .setNumberSlotsPerTaskManager(2)
                        .build()
        );
        flinkCluster.before();
    }

    @AfterAll
    static void stopFlinkCluster() {
        flinkCluster.after();
    }


    @Test
    void rxFillEventSourceRoundTrip() throws Exception {
        String topic = "rx-fill-events-" + UUID.randomUUID();
        RxFillEvent event = new RxFillEvent(
                EventType.FILL, "claim-1", "member-1", "00000000000",
                LocalDate.of(2026, Month.JANUARY, 15), 30, new BigDecimal("30.00"),
                "pharmacy-1", "rx-1", 2, Channel.RETAIL, null);
        produceFillEvent(topic, event);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaConnectionConfig kafkaConfig = new KafkaConnectionConfig(
                RED_PANDA_CONTAINER.getBootstrapServers(),
                RED_PANDA_CONTAINER.getSchemaRegistryAddress(),
                null, null, null, null);
        WatermarkConfig watermarkConfig = new WatermarkConfig(Duration.ofHours(24), Duration.ofMinutes(5));
        ParameterTool params = ParameterTool.fromMap(Map.of("kafka.topic.rx-fill-events", topic));

        DataStream<RxFillEvent> stream = RxFillEventSource.build(env, kafkaConfig, watermarkConfig, params);

        List<RxFillEvent> results = stream.executeAndCollect(1);
        assertThat(results).containsExactly(event);
    }

    @Test
    void drugClassRefSourceRoundTrip() throws Exception {
        String topic = "ndc-drug-class-ref-" + UUID.randomUUID();
        String ndcCode = "00000000000";
        DrugClassRefUpdate expected =
                new DrugClassRefUpdate(ndcCode, new DrugClassRef("CHRONIC_CARDIAC", true));

        produceDrugClassRef(topic, ndcCode, expected.drugClassRef());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaConnectionConfig kafkaConfig = new KafkaConnectionConfig(
                RED_PANDA_CONTAINER.getBootstrapServers(),
                RED_PANDA_CONTAINER.getSchemaRegistryAddress(),
                null, null, null, null);
        ParameterTool params = ParameterTool.fromMap(Map.of("kafka.topic.ndc-drug-class-ref", topic));

        DataStream<DrugClassRefUpdate> stream = DrugClassRefKafkaSource.build(env, kafkaConfig, params);

        List<DrugClassRefUpdate> results = stream.executeAndCollect(1);

        assertThat(results).containsExactly(expected);
    }

    private void produceFillEvent(String topic, RxFillEvent event) throws Exception {
        Schema schema;
        try (InputStream avsc = getClass().getResourceAsStream("/rx-fill-event.avsc")) {
            schema = new Schema.Parser().parse(avsc);
        }
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("eventType", new GenericData.EnumSymbol(
                schema.getField("eventType").schema(), event.eventType().name()));
        genericRecord.put("claimId", event.claimId());
        genericRecord.put("memberId", event.memberId());
        genericRecord.put("ndcCode", event.ndcCode());
        genericRecord.put("fillDate", (int) event.fillDate().toEpochDay());
        genericRecord.put("daySupply", event.daySupply());
        genericRecord.put("quantity", new Conversions.DecimalConversion().toBytes(
                event.quantity(), schema.getField("quantity").schema(), LogicalTypes.decimal(10, 2)));
        genericRecord.put("pharmacyId", event.pharmacyId());
        genericRecord.put("rxNumber", event.rxNumber());
        genericRecord.put("refillsAuthorized", event.refillsAuthorized());
        genericRecord.put("dispensingChannel", new GenericData.EnumSymbol(
                schema.getField("dispensingChannel").schema(), event.dispensingChanel().name()));
        genericRecord.put("originalClaimId", event.originalClaimId());

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, RED_PANDA_CONTAINER.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", RED_PANDA_CONTAINER.getSchemaRegistryAddress());

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, event.claimId(), genericRecord)).get();
        }

    }

    private void produceDrugClassRef(String topic, String ndcCode, DrugClassRef drugClassRef) throws Exception {
        Schema schema;
        try (InputStream avsc = getClass().getResourceAsStream("/drug-class-ref.avsc")) {
            schema = new Schema.Parser().parse(avsc);
        }
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("drugClass", drugClassRef.drugClass());
        genericRecord.put("trackable", drugClassRef.trackable());

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, RED_PANDA_CONTAINER.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", RED_PANDA_CONTAINER.getSchemaRegistryAddress());

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, ndcCode, genericRecord)).get();
        }
    }
}
