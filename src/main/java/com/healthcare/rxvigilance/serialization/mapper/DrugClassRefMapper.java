package com.healthcare.rxvigilance.serialization.mapper;

import com.healthcare.rxvigilance.domain.DrugClassRef;
import com.healthcare.rxvigilance.domain.DrugClassRefUpdate;
import org.apache.avro.generic.GenericRecord;

public class DrugClassRefMapper implements AvroValueMapper<DrugClassRefUpdate> {
    @Override
    public DrugClassRefUpdate map(String ndcCode, GenericRecord genericRecord) {
        DrugClassRef drugClassRef = new DrugClassRef(
                genericRecord.get("drugClass").toString(),
                (Boolean) genericRecord.get("trackable"));
        return new DrugClassRefUpdate(ndcCode, drugClassRef);
    }
}
