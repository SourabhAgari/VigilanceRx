package com.healthcare.rxvigilance.serialization.decode.decoders;

import com.healthcare.rxvigilance.domain.DrugClassRef;
import com.healthcare.rxvigilance.domain.DrugClassRefUpdate;
import com.healthcare.rxvigilance.serialization.codec.AvroRecordDecoder;
import org.apache.avro.generic.GenericRecord;

public class DrugClassRefMapper implements AvroRecordDecoder<DrugClassRefUpdate> {
    @Override
    public DrugClassRefUpdate decode(String ndcCode, GenericRecord genericRecord) {
        DrugClassRef drugClassRef = new DrugClassRef(
                genericRecord.get("drugClass").toString(),
                (Boolean) genericRecord.get("trackable"));
        return new DrugClassRefUpdate(ndcCode, drugClassRef);
    }
}
