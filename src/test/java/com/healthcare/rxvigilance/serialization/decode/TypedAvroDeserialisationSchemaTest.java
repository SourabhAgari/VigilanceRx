package com.healthcare.rxvigilance.serialization.decode;

import com.healthcare.rxvigilance.domain.DrugClassRefUpdate;
import com.healthcare.rxvigilance.serialization.decode.decoders.DrugClassRefMapper;
import com.healthcare.rxvigilance.serialization.util.KafkaSourceResult;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class TypedAvroDeserialisationSchemaTest {
    private static final String UNREACHABLE_REGISTRY_URL = "http://localhost:1";

    @Test
    void getProducedTypeReturnsWhatWasConfigured() {
        TypeInformation<KafkaSourceResult<DrugClassRefUpdate>> producedType =
                TypeInformation.of(new TypeHint<KafkaSourceResult<DrugClassRefUpdate>>() { });

        TypedAvroDeserialisationSchema<DrugClassRefUpdate> schema =
                new TypedAvroDeserialisationSchema<>(
                        UNREACHABLE_REGISTRY_URL, new DrugClassRefMapper(), Map.of(),producedType);

        assertThat(schema.getProducedType()).isEqualTo(producedType);
    }

    @Test
    void deserializeNeverThrowsEvenWhenRegistryIsUnreachable() throws Exception {
        TypedAvroDeserialisationSchema<DrugClassRefUpdate> schema = schemaAgainstUnreachableRegistry();
        schema.open(null);

        ConsumerRecord<byte[], byte[]> consRecord = new ConsumerRecord<>(
                "ndc-drug-class-ref", 0, 0L, "00069-4132-01".getBytes(), new byte[]{1, 2, 3});
        List<KafkaSourceResult<DrugClassRefUpdate>> collected = new ArrayList<>();

        assertThatCode(() -> schema.deserialize(consRecord, collectorOf(collected))).doesNotThrowAnyException();

        assertThat(collected).hasSize(1);
        assertThat(collected.get(0).isSuccess()).isFalse();
    }

    @Test
    void deserializeHandlesNullKeyWithoutThrowing() throws Exception {
        TypedAvroDeserialisationSchema<DrugClassRefUpdate> schema = schemaAgainstUnreachableRegistry();
        schema.open(null);

        ConsumerRecord<byte[], byte[]> consRecord = new ConsumerRecord<>(
                "rx-fill-events", 0, 0L, null, new byte[]{1, 2, 3});
        List<KafkaSourceResult<DrugClassRefUpdate>> collected = new ArrayList<>();

        assertThatCode(() -> schema.deserialize(consRecord, collectorOf(collected))).doesNotThrowAnyException();

        assertThat(collected).hasSize(1);
        assertThat(collected.get(0).isSuccess()).isFalse();
    }

    private TypedAvroDeserialisationSchema<DrugClassRefUpdate> schemaAgainstUnreachableRegistry() {
        return new TypedAvroDeserialisationSchema<>(
                UNREACHABLE_REGISTRY_URL, new DrugClassRefMapper(),Map.of(),
                TypeInformation.of(new TypeHint<KafkaSourceResult<DrugClassRefUpdate>>() { }));
    }

    private Collector<KafkaSourceResult<DrugClassRefUpdate>> collectorOf(
            List<KafkaSourceResult<DrugClassRefUpdate>> sink) {
        return new Collector<>() {
            @Override
            public void collect(KafkaSourceResult<DrugClassRefUpdate> rec) {
                sink.add(rec);
            }

            @Override
            public void close() {
                // No resources to release. This serializer is stateless and does not
                // own any external resources, so close() is intentionally a no-op.
            }
        };
    }

    /**
     * KafkaRecordDeserializationSchema extends Serializable — Flink ships this to every TaskManager.
     * KafkaConnectionConfig is a plain record and is NOT Serializable, so registry credentials must
     * travel as a plain map. Mirrors the encode-side guard in TypedAvroSerializationSchemaTest (#109).
     */
    @Test
    void isJavaSerializableWithRegistryCredentials() throws Exception {
        TypedAvroDeserialisationSchema<DrugClassRefUpdate> schema =
                new TypedAvroDeserialisationSchema<>(
                        UNREACHABLE_REGISTRY_URL,
                        new DrugClassRefMapper(),
                        Map.of("basic.auth.credentials.source", "USER_INFO",
                                "schema.registry.basic.auth.user.info", "user:pass"),
                        TypeInformation.of(new TypeHint<KafkaSourceResult<DrugClassRefUpdate>>() { }));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(schema);
        }

        assertThat(bytes.toByteArray()).isNotEmpty();
    }

}
