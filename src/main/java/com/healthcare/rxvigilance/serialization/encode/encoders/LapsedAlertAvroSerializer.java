package com.healthcare.rxvigilance.serialization.encode.encoders;

import com.healthcare.rxvigilance.domain.LapsedAlert;
import com.healthcare.rxvigilance.serialization.codec.AvroRecordEncoder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

public final class LapsedAlertAvroSerializer implements AvroRecordEncoder<LapsedAlert> {

    private static final Schema SCHEMA = loadSchema();

    @Override
    public GenericRecord encode(LapsedAlert alert) {
        return new GenericRecordBuilder(SCHEMA)
                .set("alertId", alert.alertId())
                .set("memberId", alert.memberId())
                .set("drugClass", alert.drugClass())
                .set("lapsedOn", (int) alert.lapsedOn().toEpochDay())
                .set("emittedAt", alert.emittedAt())
                .build();
    }

    private static Schema loadSchema() {
        try (InputStream is = LapsedAlertAvroSerializer.class.getClassLoader()
                .getResourceAsStream("lapsed-alert.avsc")) {
            return new Schema.Parser().parse(is);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load lapsed-alert.avsc", e);
        }
    }
}
