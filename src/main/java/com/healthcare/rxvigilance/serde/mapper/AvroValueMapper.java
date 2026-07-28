package com.healthcare.rxvigilance.serde.mapper;

import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;

public interface AvroValueMapper<T> extends Serializable {
    T map(String key, GenericRecord rec);
}
