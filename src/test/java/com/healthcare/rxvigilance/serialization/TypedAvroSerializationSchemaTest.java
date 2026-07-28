package com.healthcare.rxvigilance.serialization;

import com.healthcare.rxvigilance.domain.GapRiskAlert;
import com.healthcare.rxvigilance.serialization.encode.encoders.GapRiskAlertAvroSerializer;
import com.healthcare.rxvigilance.serialization.encode.TypedAvroSerializationSchema;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.Month;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TypedAvroSerializationSchemaTest {
    private static final String UNREACHABLE_REGISTRY_URL = "http://localhost:1";

    @Test
    void openBuildsWithoutThrowingEvenForUnreachableRegistry() {
        TypedAvroSerializationSchema<GapRiskAlert> schema = new TypedAvroSerializationSchema<>(
                UNREACHABLE_REGISTRY_URL, new GapRiskAlertAvroSerializer(), "gap-risk-alerts");

        assertThatCode(() -> schema.open(null)).doesNotThrowAnyException();
    }

    @Test
    void serializePropagatesSerializationExceptionRatherThanSwallowingIt() throws Exception {
        TypedAvroSerializationSchema<GapRiskAlert> schema = new TypedAvroSerializationSchema<>(
                UNREACHABLE_REGISTRY_URL, new GapRiskAlertAvroSerializer(), "gap-risk-alerts");
        schema.open(null);

        GapRiskAlert alert = new GapRiskAlert(
                "ALERT-1", "MBR-1", "INSULIN", LocalDate.of(2026,
                Month.AUGUST, 1), 5, 1_700_000_000_000L);

        assertThatThrownBy(() -> schema.serialize(alert))
                .isInstanceOf(SerializationException.class);
    }
}
