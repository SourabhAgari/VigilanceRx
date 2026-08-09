package com.healthcare.rxvigilance.integration;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.InputStream;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Properties;

public class CloudEventPublisher {
    /**
     * Publishes a small, known set of events to Redpanda Cloud so the deployed job
     * can be verified end to end (#105). Not a test - run from the IDE.
     *
     * Required environment variables:
     *   KAFKA_BROKERS, SCHEMA_REGISTRY_URL,
     *   KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD
     */
    private static final String NDC = "00093-7424-56";

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "refs";
        Properties props = cloudProducerProperties();

        switch (mode) {
            case "refs" -> {
                // Compacted topics: publish once, and the job picks these up on
                // every start. Run "fills" separately, after these have landed.
                publish(props, "ndc-drug-class-ref", null, NDC, drugClassRef("DIABETES", true));
                publish(props, "alert-lead-time-ref", null, "DIABETES|RETAIL", alertLeadTime(10));
            }
            case "fills" -> {
                LocalDate today = LocalDate.now();
                // Coverage ran out 15 days ago, so the alert instant is already past.
                publish(props, "rx-fill-events", 0, "M001",
                        fillEvent("C1", "M001", today.minusDays(45), 30));
                // Only here to move the job's event-time clock forward.
                publish(props, "rx-fill-events", 0, "M999",
                        fillEvent("C2", "M999", today, 30));
            }
            default -> throw new IllegalArgumentException(
                    "expected argument 'refs' or 'fills', got: " + mode);
        }
        System.out.println("done at " + java.time.Instant.now());
    }

    private static void publish(Properties props, String topic, Integer partition,
                                String key, GenericRecord value) throws Exception {
        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, partition, key, value)).get();
        }
        System.out.println("published to " + topic + " key=" + key);
    }

    private static GenericRecord fillEvent(String claimId, String memberId,
                                           LocalDate fillDate, int daySupply) throws Exception {
        Schema schema = schema("/rx-fill-event.avsc");
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("eventType", new GenericData.EnumSymbol(
                schema.getField("eventType").schema(), "FILL"));
        genericRecord.put("claimId", claimId);
        genericRecord.put("memberId", memberId);
        genericRecord.put("ndcCode", NDC);
        genericRecord.put("fillDate", (int) fillDate.toEpochDay());
        genericRecord.put("daySupply", daySupply);
        genericRecord.put("quantity", new Conversions.DecimalConversion().toBytes(
                new BigDecimal("30.00"),
                schema.getField("quantity").schema(),
                LogicalTypes.decimal(10, 2)));
        genericRecord.put("pharmacyId", "P1");
        genericRecord.put("rxNumber", "RX1");
        genericRecord.put("refillsAuthorized", 2);
        genericRecord.put("dispensingChannel", new GenericData.EnumSymbol(
                schema.getField("dispensingChannel").schema(), "RETAIL"));
        genericRecord.put("originalClaimId", null);
        return genericRecord;
    }

    private static GenericRecord drugClassRef(String drugClass, boolean trackable) throws Exception {
        Schema schema = schema("/drug-class-ref.avsc");
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("drugClass", drugClass);
        genericRecord.put("trackable", trackable);
        return genericRecord;
    }

    private static GenericRecord alertLeadTime(int alertLeadDays) throws Exception {
        Schema schema = schema("/alert-lead-time-ref.avsc");
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("alertLeadDays", alertLeadDays);
        return genericRecord;
    }

    private static Properties cloudProducerProperties() {
        String user = required("KAFKA_SASL_USERNAME");
        String password = required("KAFKA_SASL_PASSWORD");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, required("KAFKA_BROKERS"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        // Broker login.
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\""
                        + user + "\" password=\"" + password + "\";");

        // Schema registry login - a separate connection, needs its own credentials (D52).
        props.put("schema.registry.url", required("SCHEMA_REGISTRY_URL"));
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("schema.registry.basic.auth.user.info", user + ":" + password);

        // Schemas are registered by Terraform. #109 turned auto-registration off
        // in the job; the producer must match, or it writes a schema nobody expects.
        props.put("auto.register.schemas", false);
        props.put("use.latest.version", true);

        return props;
    }

    private static String required(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("environment variable " + name + " is not set");
        }
        return value;
    }

    private static Schema schema(String resource) throws Exception {
        try (InputStream avsc = CloudEventPublisher.class.getResourceAsStream(resource)) {
            return new Schema.Parser().parse(avsc);
        }
    }

}
