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

/**
 * Kryo serializer for Java records using record components and reflection.
 *
 * <p>Serializes record components in declaration order and reconstructs
 * records using their canonical constructor.
 */
public class RecordKryoSerializer extends Serializer<Record> {

    /**
     * Creates an immutable record serializer.
     */
    public RecordKryoSerializer() {
        setImmutable(true);
    }

    /**
     * Serializes each record component using Kryo.
     *
     * @param kryo Kryo serialization context
     * @param output destination for serialized data
     * @param rec record to serialize
     * @throws KryoException if a component cannot be accessed
     */
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

    /**
     * Deserializes a record and reconstructs it using its canonical constructor.
     *
     * @param kryo Kryo deserialization context
     * @param input source containing serialized data
     * @param aClass record type to reconstruct
     * @return reconstructed record instance
     * @throws KryoException if the record cannot be constructed
     */
    @Override
    public Record read(Kryo kryo, Input input, Class<Record> aClass) {
        RecordComponent[] components = aClass.getRecordComponents();
        Object[] args = new Object[components.length];

        for (int i = 0; i < components.length; i++) {
            args[i] = kryo.readClassAndObject(input);
        }

        try {
            // Get the types of each component
            // Store as Array so Array[Class]
            Class<?>[] paramTypes = Arrays.stream(components)
                    .map(RecordComponent::getType)
                    .toArray(Class[]::new);
            // Get the constructor of the target class
            // which accepts paramTypes as arguments
            Constructor<Record> canonicalConstructor = aClass.getDeclaredConstructor(paramTypes);
            // using constructor create the object of the class or record
            return canonicalConstructor.newInstance(args);
        } catch (ReflectiveOperationException e) {
            throw new KryoException("Failed to construct " + aClass, e);
        }
    }
}