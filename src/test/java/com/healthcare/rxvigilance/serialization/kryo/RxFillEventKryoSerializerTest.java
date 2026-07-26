package com.healthcare.rxvigilance.serialization.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.healthcare.rxvigilance.domain.Channel;
import com.healthcare.rxvigilance.domain.EventType;
import com.healthcare.rxvigilance.domain.RxFillEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.Month;

import static org.assertj.core.api.Assertions.assertThat;

class RxFillEventKryoSerializerTest {
    private final Kryo kryo = new Kryo();
    private RxFillEventKryoSerializer serializer = new RxFillEventKryoSerializer();

    @Test
    void roundTripsFillEventWithNullOriginalClaimId() {
        RxFillEvent event = new RxFillEvent(
                EventType.FILL,
                "CLM-1",
                "MBR-1",
                "NDC-1",
                LocalDate.of(2026, Month.JULY, 21), 30,
                BigDecimal.valueOf(30), "PHM-1", "RX-1", 3, Channel.MAIL_ORDER, "CLM-1");

        assertThat(roundTrip(event)).isEqualTo(event);
    }

    @Test
    void roundTripsReversalEventWithOriginalClaimId() {
        RxFillEvent event = new RxFillEvent(
                EventType.REVERSAL, "CLM-2", "MBR-1", "NDC-1", LocalDate.of(2026, Month.JULY, 21), 30,
                BigDecimal.valueOf(30), "PHM-1", "RX-1", 3, Channel.MAIL_ORDER, "CLM-1");

        assertThat(roundTrip(event)).isEqualTo(event);
    }

    private RxFillEvent roundTrip(RxFillEvent event) {
        Output output = new Output(4096);
        serializer.write(kryo, output, event);
        output.close();

        Input input = new Input(output.toBytes());
        return serializer.read(kryo, input, RxFillEvent.class);
    }
}
