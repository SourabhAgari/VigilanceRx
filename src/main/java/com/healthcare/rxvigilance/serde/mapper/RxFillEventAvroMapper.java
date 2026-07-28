package com.healthcare.rxvigilance.serde.mapper;

import com.healthcare.rxvigilance.domain.Channel;
import com.healthcare.rxvigilance.domain.EventType;
import com.healthcare.rxvigilance.domain.RxFillEvent;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;

public class RxFillEventAvroMapper implements AvroValueMapper<RxFillEvent> {
    private static final Conversions.DecimalConversion DECIMAL_CONVERSION = new Conversions.DecimalConversion();

    @Override
    public RxFillEvent map(String key, GenericRecord genericRecord) {
        Schema quantitySchema = genericRecord.getSchema().getField("quantity").schema();
        ByteBuffer quantityBytes = (ByteBuffer) genericRecord.get("quantity");
        BigDecimal quantity = DECIMAL_CONVERSION.fromBytes(
                quantityBytes, quantitySchema, LogicalTypes.decimal(10, 2));
        Object originalClaimIdRaw = genericRecord.get("originalClaimId");
        return new RxFillEvent(
                EventType.valueOf(genericRecord.get("eventType").toString()),
                genericRecord.get("claimId").toString(),
                genericRecord.get("memberId").toString(),
                genericRecord.get("ndcCode").toString(),
                LocalDate.ofEpochDay((Integer) genericRecord.get("fillDate")),
                (Integer) genericRecord.get("daySupply"),
                quantity,
                genericRecord.get("pharmacyId").toString(),
                genericRecord.get("rxNumber").toString(),
                (Integer) genericRecord.get("refillsAuthorized"),
                Channel.valueOf(genericRecord.get("dispensingChannel").toString()),
                originalClaimIdRaw == null ? null : originalClaimIdRaw.toString());
    }
}
