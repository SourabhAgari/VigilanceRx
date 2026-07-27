package com.healthcare.rxvigilance.serialization.mapper;

import com.healthcare.rxvigilance.domain.LapsedAlert;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;


class LapsedAlertAvroSerializerTest {

    private final LapsedAlertAvroSerializer serializer = new LapsedAlertAvroSerializer();

    @Test
    void serializesAllFieldsWithLapsedOnAsEpochDay() {
        LocalDate lapsedOn = LocalDate.of(2026, 8, 1);
        LapsedAlert alert = new LapsedAlert("ALERT-2", "MBR-1", "INSULIN", lapsedOn, 1_700_000_000_000L);

        GenericRecord record = serializer.serialize(alert);

        assertThat(record.get("alertId").toString()).isEqualTo("ALERT-2");
        assertThat(record.get("memberId").toString()).isEqualTo("MBR-1");
        assertThat(record.get("drugClass").toString()).isEqualTo("INSULIN");
        assertThat(record.get("lapsedOn")).isEqualTo((int) lapsedOn.toEpochDay());
        assertThat(record.get("emittedAt")).isEqualTo(1_700_000_000_000L);
    }
}
