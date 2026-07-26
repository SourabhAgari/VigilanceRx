package com.healthcare.rxvigilance.serialization.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.healthcare.rxvigilance.domain.Channel;
import com.healthcare.rxvigilance.domain.EventType;
import com.healthcare.rxvigilance.domain.RxFillEvent;

import java.math.BigDecimal;
import java.time.LocalDate;

public final class RxFillEventKryoSerializer extends Serializer<RxFillEvent> {
    public RxFillEventKryoSerializer() {
        setImmutable(true);
    }

    @Override
    public void write(Kryo kryo, Output output, RxFillEvent event) {
        output.writeString(event.eventType().name());
        output.writeString(event.claimId());
        output.writeString(event.memberId());
        output.writeString(event.ndcCode());
        output.writeLong(event.fillDate().toEpochDay());
        output.writeInt(event.daySupply());
        output.writeString(event.quantity().toPlainString());
        output.writeString(event.pharmacyId());
        output.writeString(event.rxNumber());
        output.writeInt(event.refillsAuthorized());
        output.writeString(event.dispensingChanel().name());
        kryo.writeObjectOrNull(output, event.originalClaimId(), String.class);
    }

    @Override
    public RxFillEvent read(Kryo kryo, Input input, Class<RxFillEvent> aClass) {
        return new RxFillEvent(
                EventType.valueOf(input.readString()),
                input.readString(),
                input.readString(),
                input.readString(),
                LocalDate.ofEpochDay(input.readLong()),
                input.readInt(),
                new BigDecimal(input.readString()),
                input.readString(),
                input.readString(),
                input.readInt(),
                Channel.valueOf(input.readString()),
                kryo.readObjectOrNull(input, String.class));
    }
}
