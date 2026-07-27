package com.healthcare.rxvigilance.serialization.mapper;

import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;

@FunctionalInterface
public interface AvroValueSerializer<T> extends Serializable {
    GenericRecord serialize(T value);
}
