package com.healthcare.rxvigilance.serde.mapper;

import com.healthcare.rxvigilance.domain.AlertLeadTimeUpdate;
import org.apache.avro.generic.GenericRecord;

public class AlertLeadTimeMapper implements AvroValueMapper<AlertLeadTimeUpdate> {
    @Override
    public AlertLeadTimeUpdate map(String drugClassAndChannel, GenericRecord genericRecord) {
        int alertLeadDays = (Integer) genericRecord.get("alertLeadDays");
        return new AlertLeadTimeUpdate(drugClassAndChannel, alertLeadDays);
    }
}
