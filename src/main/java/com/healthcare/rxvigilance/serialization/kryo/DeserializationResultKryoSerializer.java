package com.healthcare.rxvigilance.serialization.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.healthcare.rxvigilance.domain.RxFillEvent;
import com.healthcare.rxvigilance.serialization.DeserializationResult;

public class DeserializationResultKryoSerializer extends Serializer<DeserializationResult> {

    public DeserializationResultKryoSerializer() {
        setImmutable(true);
    }

    @Override
    public void write(Kryo kryo, Output output, DeserializationResult deserializationResult) {
        kryo.writeObjectOrNull(output, deserializationResult.event(), RxFillEvent.class);
        byte[] rawBytes = deserializationResult.rawBytes();
        if (rawBytes == null) {
            output.writeInt(-1);
        } else {
            output.writeInt(rawBytes.length);
            output.writeBytes(rawBytes);
        }
        kryo.writeObjectOrNull(output, deserializationResult.errorMessage(), String.class);
    }

    @Override
    public DeserializationResult read(Kryo kryo, Input input, Class<DeserializationResult> aClass) {
        RxFillEvent event = kryo.readObjectOrNull(input, RxFillEvent.class);
        int len = input.readInt();
        byte[] rawBytes = len < 0 ? null : input.readBytes(len);
        String errorMessage = kryo.readObjectOrNull(input, String.class);
        return new DeserializationResult(event, rawBytes, errorMessage);
    }
}
