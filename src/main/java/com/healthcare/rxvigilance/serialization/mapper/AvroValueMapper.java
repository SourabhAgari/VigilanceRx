package com.healthcare.rxvigilance.serialization.mapper;

import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;

public interface AvroValueMapper<T> extends Serializable {
    T map(String key, GenericRecord record);
}
