package com.healthcare.rxvigilance.serde.mapper;

import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;

@FunctionalInterface
public interface AvroValueSerializer<T> extends Serializable {
    GenericRecord serialize(T value);
}
