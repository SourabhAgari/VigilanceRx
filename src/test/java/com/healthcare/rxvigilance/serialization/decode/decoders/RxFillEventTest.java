package com.healthcare.rxvigilance.serialization.decode.decoders;

import com.healthcare.rxvigilance.domain.enums.Channel;
import com.healthcare.rxvigilance.domain.enums.EventType;
import com.healthcare.rxvigilance.domain.RxFillEvent;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.Month;

import static org.assertj.core.api.Assertions.assertThat;

class RxFillEventTest {
    private static final Conversions.DecimalConversion DECIMAL_CONVERSION = new Conversions.DecimalConversion();
    private final RxFillEventAvroMapper mapper = new RxFillEventAvroMapper();

    @Test
    void mpsFillEventWithNullOriginalClaimId() throws Exception {
        GenericRecord genericRecord = fillEventRecord("FILL",
                "CLM-1", LocalDate.of(2026, Month.JULY, 20),
                "RETAIL", null);
        RxFillEvent event = mapper.decode("unused-key", genericRecord);
        assertThat(event).isEqualTo(new RxFillEvent(
                EventType.FILL, "CLM-1", "MBR-1",
                "NDC-1", LocalDate.of(2026, Month.JULY, 20), 30,
                new BigDecimal("30.00"),
                "PHM-1", "RX-1", 3, Channel.RETAIL, null));
    }

    @Test
    void mapsReversalEventWithOriginalClaimId() throws IOException {
        GenericRecord genericRecord = fillEventRecord("REVERSAL", "CLM-2",
                LocalDate.of(2026, Month.JULY, 21), "MAIL_ORDER", "CLM-1");

        RxFillEvent event = mapper.decode("unused-key", genericRecord);

        assertThat(event.eventType()).isEqualTo(EventType.REVERSAL);
        assertThat(event.dispensingChanel()).isEqualTo(Channel.MAIL_ORDER);
        assertThat(event.originalClaimId()).isEqualTo("CLM-1");
    }

    private GenericRecord fillEventRecord(String eventType, String claimId, LocalDate fillDate,
                                          String channel, String originalClaimId) throws IOException {
        Schema schema = loadSchema();
        Schema quantitySchema = schema.getField("quantity").schema();
        ByteBuffer quantityBytes = DECIMAL_CONVERSION.toBytes(
                new BigDecimal("30.00"), quantitySchema, LogicalTypes.decimal(10, 2));

        return new GenericRecordBuilder(schema)
                .set("eventType", new GenericData.EnumSymbol(schema.getField("eventType").schema(), eventType))
                .set("claimId", claimId)
                .set("memberId", "MBR-1")
                .set("ndcCode", "NDC-1")
                .set("fillDate", (int) fillDate.toEpochDay())
                .set("daySupply", 30)
                .set("quantity", quantityBytes)
                .set("pharmacyId", "PHM-1")
                .set("rxNumber", "RX-1")
                .set("refillsAuthorized", 3)
                .set("dispensingChannel",
                        new GenericData.EnumSymbol(schema.getField("dispensingChannel").schema(), channel))
                .set("originalClaimId", originalClaimId)
                .build();
    }

    private Schema loadSchema() throws IOException {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("rx-fill-event.avsc")) {
            return new Schema.Parser().parse(is);
        }
    }

}
