package com.healthcare.rxvigilance.serde.mapper;

import com.healthcare.rxvigilance.domain.DrugClassRef;
import com.healthcare.rxvigilance.domain.DrugClassRefUpdate;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;

class DrugClassRefMapperTest {
    private final DrugClassRefMapper mapper = new DrugClassRefMapper();

    @Test
    void mapsNdcCodeKeyAndAvroValueIntoUpdate() throws IOException {
        GenericRecord genericRecord = drugClassRefRecord("INSULIN", true);

        DrugClassRefUpdate update = mapper.map("00069-4132-01", genericRecord);

        assertThat(update).isEqualTo(new
                DrugClassRefUpdate("00069-4132-01", new DrugClassRef("INSULIN", true)));
    }

    private GenericRecord drugClassRefRecord(String drugClass, boolean trackable) throws IOException {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("drug-class-ref.avsc")) {
            Schema schema = new Schema.Parser().parse(is);
            return new GenericRecordBuilder(schema)
                    .set("drugClass", drugClass)
                    .set("trackable", trackable)
                    .build();
        }
    }
}
