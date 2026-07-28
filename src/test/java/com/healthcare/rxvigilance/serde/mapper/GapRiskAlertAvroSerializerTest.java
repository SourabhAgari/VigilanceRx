package com.healthcare.rxvigilance.serde.mapper;

import com.healthcare.rxvigilance.domain.GapRiskAlert;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.Month;

import static org.assertj.core.api.Assertions.assertThat;

class GapRiskAlertAvroSerializerTest {

    private final GapRiskAlertAvroSerializer serializer = new GapRiskAlertAvroSerializer();

    @Test
    void serializesAllFieldsWithExpiresOnAsEpochDay(){
        LocalDate expiresOn = LocalDate.of(2026, Month.AUGUST, 1);
        GapRiskAlert alert = new GapRiskAlert("ALERT-1",
                "MBR-1", "INSULIN", expiresOn, 5, 1_700_000_000_000L);
        GenericRecord genericRecord = serializer.serialize(alert);

        assertThat(genericRecord.get("alertId").toString()).hasToString("ALERT-1");
        assertThat(genericRecord.get("memberId").toString()).hasToString("MBR-1");
        assertThat(genericRecord.get("drugClass").toString()).hasToString("INSULIN");
        assertThat(genericRecord.get("expiresOn")).isEqualTo((int) expiresOn.toEpochDay());
        assertThat(genericRecord.get("leadDays")).isEqualTo(5);
        assertThat(genericRecord.get("emittedAt")).isEqualTo(1_700_000_000_000L);
    }

}
