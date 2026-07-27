package com.healthcare.rxvigilance.serialization.mapper;

import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;

@FunctionalInterface
public interface AvrValueSerializer <T> extends Serializable {
    GenericRecord serialize(T value);
}
