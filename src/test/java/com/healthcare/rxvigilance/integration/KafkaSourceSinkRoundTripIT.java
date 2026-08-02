package com.healthcare.rxvigilance.integration;

import com.healthcare.rxvigilance.config.KafkaConnectionConfig;
import com.healthcare.rxvigilance.config.WatermarkConfig;
import com.healthcare.rxvigilance.domain.*;
import com.healthcare.rxvigilance.domain.enums.Channel;
import com.healthcare.rxvigilance.domain.enums.EventType;
import com.healthcare.rxvigilance.pipeline.sink.AlertKafkaSinks;
import com.healthcare.rxvigilance.pipeline.source.AlertLeadTimeKafkaSource;
import com.healthcare.rxvigilance.pipeline.source.DrugClassRefKafkaSource;
import com.healthcare.rxvigilance.pipeline.source.RxFillEventSource;
import com.healthcare.rxvigilance.serialization.deadletter.DeadLetterRecord;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import java.nio.charset.StandardCharsets;
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

        DataStream<RxFillEvent> stream = RxFillEventSource.build(env, kafkaConfig, watermarkConfig, params).events();

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

    @Test
    void alertLeadTimeSourceRoundTrip() throws Exception {
        String topic = "alert-lead-time-ref-" + UUID.randomUUID();
        String drugClassAndChannel = "CHRONIC_CARDIAC|RETAIL";
        AlertLeadTimeUpdate expected = new AlertLeadTimeUpdate(drugClassAndChannel, 7);

        produceAlertLeadTime(topic, drugClassAndChannel, expected.alertLeadDays());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaConnectionConfig kafkaConfig = new KafkaConnectionConfig(
                RED_PANDA_CONTAINER.getBootstrapServers(),
                RED_PANDA_CONTAINER.getSchemaRegistryAddress(),
                null, null, null, null);
        ParameterTool params = ParameterTool.fromMap(Map.of("kafka.topic.alert-lead-time-ref", topic));

        DataStream<AlertLeadTimeUpdate> stream = AlertLeadTimeKafkaSource.build(env, kafkaConfig, params);

        List<AlertLeadTimeUpdate> results = stream.executeAndCollect(1);

        assertThat(results).containsExactly(expected);
    }

    @Test
    void gapRiskAlertSinkRoundTrip() throws Exception {
        String topic = "gap-risk-alerts-" + UUID.randomUUID();
        GapRiskAlert alert = new GapRiskAlert(
                "alert-1", "member-1", "CHRONIC_CARDIAC",
                LocalDate.of(2026, Month.FEBRUARY, 1), 7, System.currentTimeMillis());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(500);

        KafkaConnectionConfig kafkaConfig = new KafkaConnectionConfig(
                RED_PANDA_CONTAINER.getBootstrapServers(),
                RED_PANDA_CONTAINER.getSchemaRegistryAddress(),
                null, null, null, null);
        ParameterTool params = ParameterTool.fromMap(Map.of("kafka.topic.gap-risk-alerts", topic));

        env.fromElements(alert)
                .sinkTo(AlertKafkaSinks.gapRiskAlertSink(env, kafkaConfig, params));
        env.execute("gap-risk-alert-sink-test");

        GenericRecord genericRecord = consumeOne(topic);

        assertThat(genericRecord.get("alertId").toString()).hasToString(alert.alertId());
        assertThat(genericRecord.get("memberId").toString()).hasToString(alert.memberId());
        assertThat(genericRecord.get("drugClass").toString()).hasToString(alert.drugClass());
        assertThat(LocalDate.ofEpochDay((Integer) genericRecord.get("expiresOn"))).isEqualTo(alert.expiresOn());
        assertThat(genericRecord.get("leadDays")).isEqualTo(alert.leadDays());
        assertThat(genericRecord.get("emittedAt")).isEqualTo(alert.emittedAt());
    }

    @Test
    void lapsedAlertSinkRoundTrip() throws Exception {
        String topic = "lapsed-alerts-" + UUID.randomUUID();
        LapsedAlert alert = new LapsedAlert(
                "alert-2", "member-2", "CHRONIC_CARDIAC",
                LocalDate.of(2026, Month.FEBRUARY, 10), System.currentTimeMillis());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(500);

        KafkaConnectionConfig kafkaConfig = new KafkaConnectionConfig(
                RED_PANDA_CONTAINER.getBootstrapServers(),
                RED_PANDA_CONTAINER.getSchemaRegistryAddress(),
                null, null, null, null);
        ParameterTool params = ParameterTool.fromMap(Map.of("kafka.topic.lapsed-alerts", topic));

        env.fromElements(alert)
                .sinkTo(AlertKafkaSinks.lapsedAlertSink(env, kafkaConfig, params));
        env.execute("lapsed-alert-sink-test");

        GenericRecord genericRecord = consumeOne(topic);

        assertThat(genericRecord.get("alertId").toString()).hasToString(alert.alertId());
        assertThat(genericRecord.get("memberId").toString()).hasToString(alert.memberId());
        assertThat(genericRecord.get("drugClass").toString()).hasToString(alert.drugClass());
        assertThat(LocalDate.ofEpochDay((Integer) genericRecord.get("lapsedOn"))).isEqualTo(alert.lapsedOn());
        assertThat(genericRecord.get("emittedAt")).isEqualTo(alert.emittedAt());
    }

    @Test
    void pdcSnapshotSinkRoundTrip() throws Exception {
        String topic = "pdc-snapshots-" + UUID.randomUUID();
        PdcSnapshot snapshot = new PdcSnapshot(
                "member-3", "CHRONIC_CARDIAC", 45,
                LocalDate.of(2026, Month.MARCH, 1), System.currentTimeMillis());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(500);

        KafkaConnectionConfig kafkaConfig = new KafkaConnectionConfig(
                RED_PANDA_CONTAINER.getBootstrapServers(),
                RED_PANDA_CONTAINER.getSchemaRegistryAddress(),
                null, null, null, null);
        ParameterTool params = ParameterTool.fromMap(Map.of("kafka.topic.pdc-snapshots", topic));

        env.fromElements(snapshot)
                .sinkTo(AlertKafkaSinks.pdcSnapshotSink(env, kafkaConfig, params));
        env.execute("pdc-snapshot-sink-test");

        GenericRecord genericRecord = consumeOne(topic);

        assertThat(genericRecord.get("memberId").toString()).hasToString(snapshot.memberId());
        assertThat(genericRecord.get("drugClass").toString()).hasToString(snapshot.drugClass());
        assertThat(genericRecord.get("totalDaysCovered")).isEqualTo(snapshot.totalDaysCovered());
        assertThat(LocalDate.ofEpochDay((Integer) genericRecord.get("currentSupplyEndDate"))).isEqualTo(snapshot.currentSupplyEndDate());
        assertThat(genericRecord.get("emittedAt")).isEqualTo(snapshot.emittedAt());
    }

    private GenericRecord consumeOne(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, RED_PANDA_CONTAINER.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url", RED_PANDA_CONTAINER.getSchemaRegistryAddress());
        props.put("specific.avro.reader", false);

        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            for (int attempt = 0; attempt < 20; attempt++) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(500));
                if (!records.isEmpty()) {
                    return records.iterator().next().value();
                }
            }
            throw new AssertionError("No record consumed from topic " + topic + " within timeout");
        }
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


    private void produceAlertLeadTime(String topic, String drugClassAndChannel, int alertLeadDays) throws Exception {
        Schema schema;
        try (InputStream avsc = getClass().getResourceAsStream("/alert-lead-time-ref.avsc")) {
            schema = new Schema.Parser().parse(avsc);
        }
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("alertLeadDays", alertLeadDays);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, RED_PANDA_CONTAINER.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", RED_PANDA_CONTAINER.getSchemaRegistryAddress());

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, drugClassAndChannel, genericRecord)).get();
        }
    }

    private ConsumerRecord<String, byte[]> consumeOneRaw(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, RED_PANDA_CONTAINER.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            for (int attempt = 0; attempt < 20; attempt++) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
                if (!records.isEmpty()) {
                    return records.iterator().next();
                }
            }
            throw new AssertionError("No record consumed from topic " + topic + " within timeout");
        }
    }

    @Test
    void deadLetterSinkRoundTrip() throws Exception {
        String topic = "dead-letter-" + UUID.randomUUID();
        byte[] rawBytes = "not-valid-avro".getBytes(StandardCharsets.UTF_8);
        String errorMessage = "Schema mismatch on decode";
        DeadLetterRecord deadLetter = new DeadLetterRecord(rawBytes, errorMessage);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(500);

        KafkaConnectionConfig kafkaConfig = new KafkaConnectionConfig(
                RED_PANDA_CONTAINER.getBootstrapServers(),
                RED_PANDA_CONTAINER.getSchemaRegistryAddress(),
                null, null, null, null);
        ParameterTool params = ParameterTool.fromMap(Map.of("kafka.topic.dead-letter", topic));

        env.fromElements(deadLetter)
                .sinkTo(AlertKafkaSinks.deadLetterSink(env, kafkaConfig, params));
        env.execute("dead-letter-sink-test");

        ConsumerRecord<String, byte[]> consumerRecord = consumeOneRaw(topic);

        assertThat(consumerRecord.value()).isEqualTo(rawBytes);
        assertThat(consumerRecord.headers().lastHeader("error-message").value())
                .isEqualTo(errorMessage.getBytes(StandardCharsets.UTF_8));
    }
}
