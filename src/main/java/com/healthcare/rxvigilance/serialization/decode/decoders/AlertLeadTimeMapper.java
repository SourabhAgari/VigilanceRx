package com.healthcare.rxvigilance.serialization.decode.decoders;

import com.healthcare.rxvigilance.domain.AlertLeadTimeUpdate;
import com.healthcare.rxvigilance.serialization.codec.AvroRecordDecoder;
import org.apache.avro.generic.GenericRecord;

public class AlertLeadTimeMapper implements AvroRecordDecoder<AlertLeadTimeUpdate> {
    @Override
    public AlertLeadTimeUpdate decode(String drugClassAndChannel, GenericRecord genericRecord) {
        int alertLeadDays = (Integer) genericRecord.get("alertLeadDays");
        return new AlertLeadTimeUpdate(drugClassAndChannel, alertLeadDays);
    }
}
