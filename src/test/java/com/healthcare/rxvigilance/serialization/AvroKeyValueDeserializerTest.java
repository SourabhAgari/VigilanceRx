package com.healthcare.rxvigilance.serialization;

import com.healthcare.rxvigilance.domain.DrugClassRef;
import com.healthcare.rxvigilance.domain.DrugClassRefUpdate;
import com.healthcare.rxvigilance.serialization.decode.AvroKeyValueDeSerializer;
import com.healthcare.rxvigilance.serialization.decode.decoders.DrugClassRefMapper;
import com.healthcare.rxvigilance.serialization.util.KafkaSourceResult;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;

class AvroKeyValueDeserializerTest {

    private final SchemaRegistryClient registryClient = new MockSchemaRegistryClient();
    private final KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer(registryClient);
    private final AvroKeyValueDeSerializer<DrugClassRefUpdate> deserializer =
            new AvroKeyValueDeSerializer<>(registryClient, new DrugClassRefMapper());

    @Test
    void deserializeCombinesKeyAndRegistryDecodedValueOnSuccess() throws IOException, RestClientException {
        Schema schema = loadSchema();
        registryClient.register("ndc-drug-class-ref-value", schema);

        byte[] valueBytes = avroSerializer.serialize("ndc-drug-class-ref", drugClassRefRecord(schema, "INSULIN", true));

        KafkaSourceResult<DrugClassRefUpdate> result = deserializer.deserialize("00069-4132-01", valueBytes);

        assertThat(result).isEqualTo(KafkaSourceResult.success(
                new DrugClassRefUpdate("00069-4132-01", new DrugClassRef("INSULIN", true))));
    }

    @Test
    void deserializeRoutesCorruptBytesToFailureInsteadOfThrowing() {
        byte[] corrupted = {0x05, 0x00, 0x00, 0x00, 0x01};

        KafkaSourceResult<DrugClassRefUpdate> result = deserializer.deserialize("00069-4132-01", corrupted);

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.rawBytes()).isEqualTo(corrupted);
        assertThat(result.errorMessage()).isNotNull();
    }

    private Schema loadSchema() throws IOException {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("drug-class-ref.avsc")) {
            return new Schema.Parser().parse(is);
        }
    }

    private GenericRecord drugClassRefRecord(Schema schema, String drugClass, boolean trackable) {
        return new GenericRecordBuilder(schema)
                .set("drugClass", drugClass)
                .set("trackable", trackable)
                .build();
    }
}
