package com.healthcare.rxvigilance.serialization.decode.decoders;

import com.healthcare.rxvigilance.domain.AlertLeadTimeUpdate;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;

class AlertLeadTimeTest {
    private final AlertLeadTimeMapper mapper = new AlertLeadTimeMapper();

    @Test
    void mapsCompositeKeyAndAvroValueIntoUpdate() throws IOException {
        GenericRecord genericRecord = alertLeadTimeRecord(7);

        AlertLeadTimeUpdate update = mapper.decode("INSULIN|MAIL_ORDER", genericRecord);

        assertThat(update).isEqualTo(new AlertLeadTimeUpdate("INSULIN|MAIL_ORDER", 7));
    }

    private GenericRecord alertLeadTimeRecord(int alertLeadDays) throws IOException {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("alert-lead-time-ref.avsc")) {
            Schema schema = new Schema.Parser().parse(is);
            return new GenericRecordBuilder(schema)
                    .set("alertLeadDays", alertLeadDays)
                    .build();
        }
    }
}
