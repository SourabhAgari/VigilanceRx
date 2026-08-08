package com.healthcare.rxvigilance.serialization.encode;

import com.healthcare.rxvigilance.domain.GapRiskAlert;
import com.healthcare.rxvigilance.serialization.encode.encoders.GapRiskAlertAvroSerializer;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.time.LocalDate;
import java.time.Month;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class TypedAvroSerializationSchemaTest {
    private static final String UNREACHABLE_REGISTRY_URL = "http://localhost:1";

    @Test
    void openBuildsWithoutThrowingEvenForUnreachableRegistry() {
        TypedAvroSerializationSchema<GapRiskAlert> schema = new TypedAvroSerializationSchema<>(
                UNREACHABLE_REGISTRY_URL, Map.of(),new GapRiskAlertAvroSerializer(), "gap-risk-alerts");

        assertThatCode(() -> schema.open(null)).doesNotThrowAnyException();
    }

    @Test
    void serializePropagatesSerializationExceptionRatherThanSwallowingIt() throws Exception {
        TypedAvroSerializationSchema<GapRiskAlert> schema = new TypedAvroSerializationSchema<>(
                UNREACHABLE_REGISTRY_URL,Map.of(), new GapRiskAlertAvroSerializer(), "gap-risk-alerts");
        schema.open(null);

        GapRiskAlert alert = new GapRiskAlert(
                "ALERT-1", "MBR-1", "INSULIN", LocalDate.of(2026,
                Month.AUGUST, 1), 5, 1_700_000_000_000L);

        assertThatThrownBy(() -> schema.serialize(alert))
                .isInstanceOf(SerializationException.class);
    }

    /**
     * SerializationSchema extends Serializable — Flink ships this object to every TaskManager.
     * KafkaConnectionConfig is a plain record and is NOT Serializable, so the registry credentials
     * must travel as a plain map. This test fails loudly if someone swaps the record back in.
     */
    @Test
    void isJavaSerializableWithRegistryCredentials() throws Exception {
        TypedAvroSerializationSchema<GapRiskAlert> schema = new TypedAvroSerializationSchema<>(
                UNREACHABLE_REGISTRY_URL,
                Map.of("basic.auth.credentials.source", "USER_INFO",
                        "schema.registry.basic.auth.user.info", "user:pass"),
                new GapRiskAlertAvroSerializer(),
                "gap-risk-alerts");

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(schema);
        }

        assertThat(bytes.toByteArray()).isNotEmpty();
    }
}
