package com.healthcare.rxvigilance.serialization.codec;

import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;

@FunctionalInterface
public interface AvroRecordEncoder<T> extends Serializable {
    GenericRecord encode(T value);
}
