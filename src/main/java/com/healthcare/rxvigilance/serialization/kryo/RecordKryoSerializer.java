package com.healthcare.rxvigilance.serialization.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RecordKryoSerializer extends Serializer<Record> {

    public RecordKryoSerializer() {
        setImmutable(true);
    }

    @Override
    public void write(Kryo kryo, Output output, Record rec) {
        for (RecordComponent component : rec.getClass().getRecordComponents()) {
            try {
                Object value = component.getAccessor().invoke(rec);
                if (value instanceof List<?> list) {
                    value = new ArrayList<>(list);
                }
                kryo.writeClassAndObject(output, value);
            } catch (ReflectiveOperationException e) {
                throw new KryoException("Failed to read component " + component.getName()
                        + " of " + rec.getClass(), e);
            }
        }
    }

    @Override
    public Record read(Kryo kryo, Input input, Class<Record> aClass) {
        RecordComponent[] components = aClass.getRecordComponents();
        Object[] args = new Object[components.length];

        for (int i = 0; i < components.length; i++) {
            args[i] = kryo.readClassAndObject(input);
        }

        try {
            Class<?>[] paramTypes = Arrays.stream(components)
                    .map(RecordComponent::getType)
                    .toArray(Class[]::new);
            Constructor<Record> canonicalConstructor = aClass.getDeclaredConstructor(paramTypes);
            return canonicalConstructor.newInstance(args);
        } catch (ReflectiveOperationException e) {
            throw new KryoException("Failed to construct " + aClass, e);
        }
    }
}
