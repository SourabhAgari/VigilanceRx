package com.healthcare.rxvigilance.serialization.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.healthcare.rxvigilance.domain.*;
import org.junit.jupiter.api.Test;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.Month;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecordKryoSerializerTest {
    private final Kryo kryo = new Kryo();
    private final RecordKryoSerializer serializer = new RecordKryoSerializer();

    RecordKryoSerializerTest() {
        Kryo.DefaultInstantiatorStrategy instantiatorStrategy = new Kryo.DefaultInstantiatorStrategy();
        instantiatorStrategy.setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
        kryo.setInstantiatorStrategy(instantiatorStrategy);
    }

    @Test
    void roundTripsSimpleRecordWithNoNestedTypes() {
        kryo.register(AlertLeadTimeUpdate.class, serializer);

        AlertLeadTimeUpdate update = new AlertLeadTimeUpdate("INSULIN|MAIL_ORDER", 7);

        assertThat(roundTrip(update, AlertLeadTimeUpdate.class)).isEqualTo(update);
    }

    @Test
    void roundTripsRecordWithNestedRecordField() {
        kryo.register(DrugClassRef.class, serializer);
        kryo.register(DrugClassRefUpdate.class, serializer);

        DrugClassRefUpdate update = new DrugClassRefUpdate("00069-4132-01",
                new DrugClassRef("INSULIN", true));

        assertThat(roundTrip(update, DrugClassRefUpdate.class)).isEqualTo(update);
    }

    @Test
    void roundTripsRecordWithNullField() {
        kryo.register(RxFillEvent.class, serializer);

        RxFillEvent event = new RxFillEvent(
                EventType.FILL, "CLM-1", "MBR-1", "NDC-1",
                LocalDate.of(2026, Month.JULY, 20), 30,
                new BigDecimal("30.00"), "PHM-1", "RX-1",
                3, Channel.RETAIL, null);

        assertThat(roundTrip(event, RxFillEvent.class)).isEqualTo(event);
    }

    @Test
    void roundTripsRecordWithPopulatedNullableField() {
        kryo.register(RxFillEvent.class, serializer);

        RxFillEvent event = new RxFillEvent(
                EventType.REVERSAL, "CLM-2", "MBR-1", "NDC-1",
                LocalDate.of(2026, Month.JULY, 21), 30,
                new BigDecimal("30.00"), "PHM-1", "RX-1",
                3, Channel.MAIL_ORDER, "CLM-1");

        assertThat(roundTrip(event, RxFillEvent.class)).isEqualTo(event);
    }

    private <T> T roundTrip(T value, Class<T> type) {
        Output output = new Output(4096);
        serializer.write(kryo, output, (Record) value);
        output.close();

        Input input = new Input(output.toBytes());
        @SuppressWarnings("unchecked")
        T result = (T) serializer.read(kryo, input, (Class<Record>) (Class<?>) type);
        return result;
    }

    @Test
    void readWrapsReflectiveFailureAsKryoException() {
        kryo.register(OddOnlyRecord.class, serializer);

        Output output = new Output(4096);
        kryo.writeClassAndObject(output, 4); // even -> OddOnlyRecord's compact constructor rejects it
        output.close();
        Input input = new Input(output.toBytes());

        @SuppressWarnings("unchecked")
        Class<Record> oddOnlyAsRecord = (Class<Record>) (Class<?>) OddOnlyRecord.class;

        assertThatThrownBy(() -> serializer.read(kryo, input, oddOnlyAsRecord))
                .isInstanceOf(KryoException.class)
                .hasMessageContaining("Failed to construct");
    }

    @Test
    void writeWrapsReflectiveFailureAsKryoException() {
        kryo.register(ThrowingAccessorRecord.class, serializer);
        Output output = new Output(4096);
        ThrowingAccessorRecord throwingAccessorRecord = new ThrowingAccessorRecord(1);

        assertThatThrownBy(() -> serializer.write(kryo, output, throwingAccessorRecord))
                .isInstanceOf(KryoException.class)
                .hasMessageContaining("Failed to read component");
    }

    private record ThrowingAccessorRecord(int value) {
        @Override
        public int value() {
            throw new RuntimeException("accessor boom");
        }
    }

    private record OddOnlyRecord(int value) {
        OddOnlyRecord {
            if (value % 2 == 0) {
                throw new IllegalArgumentException("value must be odd");
            }
        }
    }
}
