package com.healthcare.rxvigilance.integration;

import com.healthcare.rxvigilance.AdherenceJob;
import com.healthcare.rxvigilance.config.JobConfig;
import com.healthcare.rxvigilance.domain.DrugClassRef;
import com.healthcare.rxvigilance.domain.RxFillEvent;
import com.healthcare.rxvigilance.domain.enums.Channel;
import com.healthcare.rxvigilance.domain.enums.EventType;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.time.Month;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class AdherencePipelineIT {

    @Container
    static final RedpandaContainer RED_PANDA_CONTAINER =
            new RedpandaContainer(DockerImageName.parse("redpandadata/redpanda:v24.1.7"));

    static MiniClusterWithClientResource flinkCluster;
    @TempDir
    Path tempDir;

    @BeforeAll
    static void startFlinkCluster() throws Exception {
        flinkCluster = new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setNumberTaskManagers(1)
                        .setNumberSlotsPerTaskManager(2)
                        .build());
        flinkCluster.before();
    }

    @AfterAll
    static void stopFlinkCluster() {
        flinkCluster.after();
    }

    // per-test: JobClient handle, cancelled in @AfterEach
    private JobClient jobClient;

    @AfterEach
    void cancelJob() throws Exception {
        if (jobClient != null) {
            jobClient.cancel().get();
        }
    }

    @Test
    void fillProducesGapRiskAlertAndPdcSnapshot() throws Exception {
        String fillTopic = "rx-fill-events-" + UUID.randomUUID();
        String ndcTopic = "ndc-drug-class-ref-" + UUID.randomUUID();
        String leadTimeTopic = "alert-lead-time-ref-" + UUID.randomUUID();
        String gapRiskTopic = "gap-risk-alerts-" + UUID.randomUUID();
        String pdcTopic = "pdc-snapshots-" + UUID.randomUUID();

        registerSubject(gapRiskTopic, "/gap-risk-alert.avsc");
        registerSubject(pdcTopic, "/pdc-snapshot.avsc");

        produceDrugClassRef(ndcTopic, "00093-7424-56", new DrugClassRef("DIABETES", true));
        produceAlertLeadTime(leadTimeTopic, "DIABETES|RETAIL", 10);
        RxFillEvent fill = new RxFillEvent(
                EventType.FILL, "claim-it-1", "member-it-1", "00093-7424-56",
                LocalDate.of(2026, Month.AUGUST, 2), 5, new BigDecimal("5.00"),
                "pharmacy-1", "rx-1", 0, Channel.RETAIL, null);
        produceFillEvent(fillTopic, fill);

        jobClient = startJob(Map.of(
                "kafka.topic.rx-fill-events", fillTopic,
                "kafka.topic.ndc-drug-class-ref", ndcTopic,
                "kafka.topic.alert-lead-time-ref", leadTimeTopic,
                "kafka.topic.gap-risk-alerts", gapRiskTopic,
                "kafka.topic.dead-letter",   "dead-letter-"   + UUID.randomUUID(),
                "kafka.topic.lapsed-alerts", "lapsed-alerts-" + UUID.randomUUID(),
                "kafka.topic.pdc-snapshots", pdcTopic), tempDir);

        // PdcSnapshot is emitted synchronously from processElement, so this proves the first fill
        // was consumed and enriched. It also blocks until a checkpoint has committed the sink
        // transaction and the verifying consumer has joined its group — comfortably longer than
        // watermark.idleness.ms, which is what the next step depends on.
        GenericRecord snapshot = consumeOne(pdcTopic);
        assertThat(snapshot.get("memberId")).hasToString("member-it-1");

        // GapRiskAlert is emitted only from onTimer, so the operator's watermark has to pass
        // 2026-07-28. Both broadcast sources use noWatermarks(), so their channels sit at
        // Long.MIN_VALUE and hold the operator's min-watermark down until they are marked IDLE
        // and excluded. Flink will not advance a watermark while *every* channel is idle, so the
        // fill channel must be active after the broadcasts have gone idle. Sending this fill now
        // — rather than before the job starts — makes that ordering deterministic.
        RxFillEvent watermarkAdvancer = new RxFillEvent(
                EventType.FILL, "claim-it-2", "member-it-2", "00093-7424-56",
                LocalDate.of(2026, Month.AUGUST, 10), 5, new BigDecimal("5.00"),
                "pharmacy-1", "rx-2", 0, Channel.RETAIL, null);
        produceFillEvent(fillTopic, watermarkAdvancer);

        GenericRecord alert = consumeOne(gapRiskTopic);
        assertThat(alert.get("memberId")).hasToString("member-it-1");
        assertThat(alert.get("drugClass")).hasToString("DIABETES");
        assertThat(alert.get("leadDays")).isEqualTo(10);
    }

    @Test
    void reversalWithNoRemainingCoverageProducesLapsedAlert() throws Exception {
        String fillTopic = "rx-fill-events-" + UUID.randomUUID();
        String ndcTopic = "ndc-drug-class-ref-" + UUID.randomUUID();
        String leadTimeTopic = "alert-lead-time-ref-" + UUID.randomUUID();
        String lapsedTopic = "lapsed-alerts-" + UUID.randomUUID();
        String pdcTopic = "pdc-snapshots-" + UUID.randomUUID();

        registerSubject(lapsedTopic, "/lapsed-alert.avsc");
        registerSubject(pdcTopic, "/pdc-snapshot.avsc");

        produceDrugClassRef(ndcTopic, "00093-7424-56", new DrugClassRef("DIABETES", true));
        produceAlertLeadTime(leadTimeTopic, "DIABETES|RETAIL", 10);

        RxFillEvent fill = new RxFillEvent(
                EventType.FILL, "claim-it-3", "member-it-3", "00093-7424-56",
                LocalDate.of(2026, Month.AUGUST, 2), 5, new BigDecimal("5.00"),
                "pharmacy-1", "rx-3", 0, Channel.RETAIL, null);
        produceFillEvent(fillTopic, fill);

        RxFillEvent reversal = new RxFillEvent(
                EventType.REVERSAL, "claim-it-3-rev", "member-it-3", "00093-7424-56",
                LocalDate.of(2026, Month.AUGUST, 3), 5, new BigDecimal("5.00"),
                "pharmacy-1", "rx-3", 0, Channel.RETAIL, "claim-it-3");
        produceFillEvent(fillTopic, reversal);

        jobClient = startJob(Map.of(
                "kafka.topic.rx-fill-events", fillTopic,
                "kafka.topic.ndc-drug-class-ref", ndcTopic,
                "kafka.topic.alert-lead-time-ref", leadTimeTopic,
                "kafka.topic.lapsed-alerts", lapsedTopic,
                "kafka.topic.pdc-snapshots", pdcTopic,
                "kafka.topic.gap-risk-alerts", "gap-risk-alerts-" + UUID.randomUUID(),
                "kafka.topic.dead-letter", "dead-letter-" + UUID.randomUUID()), tempDir);

        GenericRecord alert = consumeOne(lapsedTopic);
        assertThat(alert.get("memberId")).hasToString("member-it-3");
        assertThat(alert.get("drugClass")).hasToString("DIABETES");
    }

    @Test
    void malformedRecordsOnAllThreeSourcesRouteToSharedDeadLetterTopic() throws Exception {
        String fillTopic = "rx-fill-events-" + UUID.randomUUID();
        String ndcTopic = "ndc-drug-class-ref-" + UUID.randomUUID();
        String leadTimeTopic = "alert-lead-time-ref-" + UUID.randomUUID();
        String deadLetterTopic = "dead-letter-" + UUID.randomUUID();

        produceMalformed(fillTopic, "not-valid-avro-fill");
        produceMalformed(ndcTopic, "not-valid-avro-ndc");
        produceMalformed(leadTimeTopic, "not-valid-avro-lead-time");

        jobClient = startJob(Map.of(
                "kafka.topic.rx-fill-events", fillTopic,
                "kafka.topic.ndc-drug-class-ref", ndcTopic,
                "kafka.topic.alert-lead-time-ref", leadTimeTopic,
                "kafka.topic.dead-letter", deadLetterTopic,
                "kafka.topic.gap-risk-alerts", "gap-risk-alerts-" + UUID.randomUUID(),
                "kafka.topic.pdc-snapshots", "pdc-snapshots-" + UUID.randomUUID(),
                "kafka.topic.lapsed-alerts", "lapsed-alerts-" + UUID.randomUUID()), tempDir);

        List<String> errorMessages = consumeRaw(deadLetterTopic, 3);
        assertThat(errorMessages).hasSize(3).doesNotContainNull();
    }

    private JobClient startJob(Map<String, String> topicOverrides, Path checkpointDir) throws Exception {
        Map<String, String> allArgs = new HashMap<>(topicOverrides);
        allArgs.put("kafka.brokers", RED_PANDA_CONTAINER.getBootstrapServers());
        allArgs.put("schema.registry.url", RED_PANDA_CONTAINER.getSchemaRegistryAddress());
        allArgs.put("checkpoint.dir", "file://" + checkpointDir.toAbsolutePath());
        allArgs.put("watermark.idleness.ms", "500");
        allArgs.put("alert.lead.days.default", "10");
        allArgs.put("checkpoint.interval.ms", "500");
        allArgs.put("checkpoint.min.pause.ms", "0");
        allArgs.put("watermark.out.of.orderness.ms", "0");

        List<String> argList = new ArrayList<>();
        allArgs.forEach((k, v) -> { argList.add("--" + k); argList.add(v); });

        JobConfig jobConfig = JobConfig.fromArgs(argList.toArray(new String[0]));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        AdherenceJob.buildTopology(env, jobConfig);
        return env.executeAsync("adherence-job-it");
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

    /**
     * With AUTO_REGISTER_SCHEMAS off (#109), the job looks a schema up rather than creating it.
     * These tests use randomised topic names, so no external tool can pre-register the subject —
     * the test must do it, exactly as Terraform does for the real topics.
     */
    private static void registerSubject(String topic, String avscResource) throws Exception {
        SchemaRegistryClient client = new CachedSchemaRegistryClient(
                RED_PANDA_CONTAINER.getSchemaRegistryAddress(), 10);
        try (InputStream in = AdherencePipelineIT.class.getResourceAsStream(avscResource)) {
            client.register(topic + "-value", new AvroSchema(new Schema.Parser().parse(in)));
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

    public void produceDrugClassRef(String topic, String ndcCode, DrugClassRef drugClassRef) throws Exception {
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
            for (int attempt = 0; attempt < 120; attempt++) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(500));
                if (!records.isEmpty()) {
                    return records.iterator().next().value();
                }
            }
            throw new AssertionError("No record consumed from topic " + topic + " within timeout");
        }
    }

    private void produceMalformed(String topic, String garbage) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, RED_PANDA_CONTAINER.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, "bad-key", garbage.getBytes())).get();
        }
    }

    private List<String> consumeRaw(String topic, int expectedCount) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, RED_PANDA_CONTAINER.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());

        List<String> errorMessages = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            for (int attempt = 0; attempt < 120 && errorMessages.size() < expectedCount; attempt++) {
                var records = consumer.poll(Duration.ofMillis(500));
                records.forEach(r -> {
                    var header = r.headers().lastHeader("error-message");
                    errorMessages.add(header == null ? null : new String(header.value()));
                });
            }
            return errorMessages;
        }
    }

}
