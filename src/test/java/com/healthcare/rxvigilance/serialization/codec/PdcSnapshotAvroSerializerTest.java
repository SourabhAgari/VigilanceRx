package com.healthcare.rxvigilance.serialization.codec;

import com.healthcare.rxvigilance.domain.PdcSnapshot;
import com.healthcare.rxvigilance.serialization.encode.encoders.PdcSnapshotAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.Month;

import static org.assertj.core.api.Assertions.assertThat;

class PdcSnapshotAvroSerializerTest {
    private final PdcSnapshotAvroSerializer serializer = new PdcSnapshotAvroSerializer();

    @Test
    void serializesAllFieldsWithCurrentSupplyEndDateAsEpochDay() {
        LocalDate currentSupplyEndDate = LocalDate.of(2026, Month.AUGUST, 1);
        PdcSnapshot snapshot = new PdcSnapshot("MBR-1", "INSULIN", 45, currentSupplyEndDate, 1_700_000_000_000L);

        GenericRecord genericRecord = serializer.encode(snapshot);

        assertThat(genericRecord.get("memberId").toString()).hasToString("MBR-1");
        assertThat(genericRecord.get("drugClass").toString()).hasToString("INSULIN");
        assertThat(genericRecord.get("totalDaysCovered")).isEqualTo(45);
        assertThat(genericRecord.get("currentSupplyEndDate")).isEqualTo((int) currentSupplyEndDate.toEpochDay());
        assertThat(genericRecord.get("emittedAt")).isEqualTo(1_700_000_000_000L);
    }
}
