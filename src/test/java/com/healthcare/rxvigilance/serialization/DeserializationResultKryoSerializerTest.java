package com.healthcare.rxvigilance.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.healthcare.rxvigilance.domain.Channel;
import com.healthcare.rxvigilance.domain.EventType;
import com.healthcare.rxvigilance.domain.RxFillEvent;
import com.healthcare.rxvigilance.serialization.kryo.DeserializationResultKryoSerializer;
import com.healthcare.rxvigilance.serialization.kryo.RxFillEventKryoSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.bouncycastle.util.Bytes;

import java.math.BigDecimal;
import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;

class DeserializationResultKryoSerializerTest {
    Kryo kryo = new Kryo();
    private final DeserializationResultKryoSerializer serializer = new DeserializationResultKryoSerializer();

    @Test
    void roundTripsSuccessResult() {
        kryo.register(RxFillEvent.class, new RxFillEventKryoSerializer());
        RxFillEvent event = new RxFillEvent(
                EventType.FILL, "CLM-1", "MBR-1", "NDC-1", LocalDate.of(2026, 7, 20), 30,
                BigDecimal.valueOf(30), "PHM-1", "RX-1", 3, Channel.RETAIL, null);
        DeserializationResult result = DeserializationResult.success(event);

        assertThat(roundTrip(result)).isEqualTo(result);
    }

    @Test
    void roundTripsFailureResult() {
        DeserializationResult result = DeserializationResult.failure(new byte[]{1,2,3},"bad magic byte");
        kryo.register(RxFillEvent.class, new RxFillEventKryoSerializer());
        assertThat(roundTrip(result)).isEqualTo(result);
    }

    private DeserializationResult roundTrip(DeserializationResult deserializationResult) {
        Output output = new Output(4096);
        serializer.write(kryo, output, deserializationResult);
        output.close();

        return serializer.read(kryo, new Input(output.toBytes()), DeserializationResult.class);
    }
}
