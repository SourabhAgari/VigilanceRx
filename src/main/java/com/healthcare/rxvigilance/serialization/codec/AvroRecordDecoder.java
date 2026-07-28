package com.healthcare.rxvigilance.serialization.codec;

import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;

public interface AvroRecordDecoder<T> extends Serializable {
    T decode(String key, GenericRecord rec);
}
