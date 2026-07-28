package com.healthcare.rxvigilance.serialization.encode.encoders;

import com.healthcare.rxvigilance.domain.GapRiskAlert;
import com.healthcare.rxvigilance.serialization.codec.AvroRecordEncoder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

public final class GapRiskAlertAvroSerializer implements AvroRecordEncoder<GapRiskAlert> {

    private static final Schema SCHEMA = loadSchema();

    @Override
    public GenericRecord encode(GapRiskAlert alert) {
        return new GenericRecordBuilder(SCHEMA)
                .set("alertId", alert.alertId())
                .set("memberId", alert.memberId())
                .set("drugClass", alert.drugClass())
                .set("expiresOn", (int) alert.expiresOn().toEpochDay())
                .set("leadDays", alert.leadDays())
                .set("emittedAt", alert.emittedAt())
                .build();
    }

    private static Schema loadSchema() {
        try (InputStream is = GapRiskAlertAvroSerializer.class.getClassLoader()
                .getResourceAsStream("gap-risk-alert.avsc")) {
            return new Schema.Parser().parse(is);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load gap-risk-alert.avsc", e);
        }
    }
}