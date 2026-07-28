package com.healthcare.rxvigilance.serialization.encode.encoders;

import com.healthcare.rxvigilance.domain.PdcSnapshot;
import com.healthcare.rxvigilance.serialization.codec.AvroRecordEncoder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

public final class PdcSnapshotAvroSerializer implements AvroRecordEncoder<PdcSnapshot> {

    private static final Schema SCHEMA = loadSchema();

    @Override
    public GenericRecord encode(PdcSnapshot snapshot) {
        return new GenericRecordBuilder(SCHEMA)
                .set("memberId", snapshot.memberId())
                .set("drugClass", snapshot.drugClass())
                .set("totalDaysCovered", snapshot.totalDaysCovered())
                .set("currentSupplyEndDate", (int) snapshot.currentSupplyEndDate().toEpochDay())
                .set("emittedAt", snapshot.emittedAt())
                .build();
    }

    private static Schema loadSchema() {
        try (InputStream is = PdcSnapshotAvroSerializer.class.getClassLoader()
                .getResourceAsStream("pdc-snapshot.avsc")) {
            return new Schema.Parser().parse(is);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load pdc-snapshot.avsc", e);
        }
    }
}
