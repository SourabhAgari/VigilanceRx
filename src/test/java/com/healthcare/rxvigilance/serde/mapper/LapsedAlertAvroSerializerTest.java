package com.healthcare.rxvigilance.serde.mapper;

import com.healthcare.rxvigilance.domain.LapsedAlert;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.Month;

import static org.assertj.core.api.Assertions.assertThat;


class LapsedAlertAvroSerializerTest {

    private final LapsedAlertAvroSerializer serializer = new LapsedAlertAvroSerializer();

    @Test
    void serializesAllFieldsWithLapsedOnAsEpochDay() {
        LocalDate lapsedOn = LocalDate.of(2026, Month.AUGUST, 1);
        LapsedAlert alert = new LapsedAlert("ALERT-2", "MBR-1", "INSULIN", lapsedOn, 1_700_000_000_000L);

        GenericRecord genericRecord = serializer.serialize(alert);

        assertThat(genericRecord.get("alertId").toString()).hasToString("ALERT-2");
        assertThat(genericRecord.get("memberId").toString()).hasToString("MBR-1");
        assertThat(genericRecord.get("drugClass").toString()).hasToString("INSULIN");
        assertThat(genericRecord.get("lapsedOn")).isEqualTo((int) lapsedOn.toEpochDay());
        assertThat(genericRecord.get("emittedAt")).isEqualTo(1_700_000_000_000L);
    }
}
