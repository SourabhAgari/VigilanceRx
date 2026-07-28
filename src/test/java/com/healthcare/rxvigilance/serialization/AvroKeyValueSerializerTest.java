package com.healthcare.rxvigilance.serialization;

import com.healthcare.rxvigilance.domain.GapRiskAlert;
import com.healthcare.rxvigilance.serialization.encode.encoders.GapRiskAlertAvroSerializer;
import com.healthcare.rxvigilance.serialization.encode.AvroKeyValueSerializer;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.Month;

import static org.assertj.core.api.Assertions.assertThat;

class AvroKeyValueSerializerTest {
    private final SchemaRegistryClient registryClient = new MockSchemaRegistryClient();
    private final AvroKeyValueSerializer<GapRiskAlert> serializer = new AvroKeyValueSerializer<>(registryClient,
            new GapRiskAlertAvroSerializer(),"gap-risk-alerts");

    @Test
    void serializeProducesRegistryDecodableBytes() throws IOException, RestClientException {
        registryClient.register("gap-risk-alerts-value", loadSchema());

        LocalDate expiresOn = LocalDate.of(2026, Month.AUGUST, 1);
        GapRiskAlert alert = new GapRiskAlert("ALERT-1", "MBR-1", "INSULIN", expiresOn, 5, 1_700_000_000_000L);

        byte[] bytes = serializer.serialize(alert);
        KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(registryClient);
        GenericRecord decoded = (GenericRecord) deserializer.deserialize("gap-risk-alerts", bytes);

        assertThat(decoded.get("alertId").toString()).hasToString("ALERT-1");
        assertThat(decoded.get("memberId").toString()).hasToString("MBR-1");
        assertThat(decoded.get("drugClass").toString()).hasToString("INSULIN");
        assertThat(decoded.get("expiresOn")).isEqualTo((int) expiresOn.toEpochDay());
        assertThat(decoded.get("leadDays")).isEqualTo(5);
        assertThat(decoded.get("emittedAt")).isEqualTo(1_700_000_000_000L);
    }

    private Schema loadSchema() throws IOException {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("gap-risk-alert.avsc")) {
            return new Schema.Parser().parse(is);
        }
    }
}
