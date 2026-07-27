package com.healthcare.rxvigilance.serialization.mapper;

import com.healthcare.rxvigilance.domain.PdcSnapshot;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;

public class PdcSnapshotAvroSerializerTest {
    private final PdcSnapshotAvroSerializer serializer = new PdcSnapshotAvroSerializer();

    @Test
    void serializesAllFieldsWithCurrentSupplyEndDateAsEpochDay() {
        LocalDate currentSupplyEndDate = LocalDate.of(2026, 8, 1);
        PdcSnapshot snapshot = new PdcSnapshot("MBR-1", "INSULIN", 45, currentSupplyEndDate, 1_700_000_000_000L);

        GenericRecord record = serializer.serialize(snapshot);

        assertThat(record.get("memberId").toString()).isEqualTo("MBR-1");
        assertThat(record.get("drugClass").toString()).isEqualTo("INSULIN");
        assertThat(record.get("totalDaysCovered")).isEqualTo(45);
        assertThat(record.get("currentSupplyEndDate")).isEqualTo((int) currentSupplyEndDate.toEpochDay());
        assertThat(record.get("emittedAt")).isEqualTo(1_700_000_000_000L);
    }
}
