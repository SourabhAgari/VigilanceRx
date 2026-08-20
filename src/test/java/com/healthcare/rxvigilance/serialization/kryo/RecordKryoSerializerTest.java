package com.healthcare.rxvigilance.serialization.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.healthcare.rxvigilance.domain.*;
import com.healthcare.rxvigilance.domain.enums.Channel;
import com.healthcare.rxvigilance.domain.enums.EventType;
import org.junit.jupiter.api.Test;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.Month;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests serialization and deserialization of Java records using Kryo.
 *
 * <p>Verifies round-trip behavior for simple records, nested records, nullable
 * fields, and collection fields, as well as error handling for failures during
 * record construction and component access.
 */
class RecordKryoSerializerTest {
    private final Kryo kryo = new Kryo();
    private final RecordKryoSerializer serializer = new RecordKryoSerializer();
    /**
     * Configures Kryo to use its default instantiation strategy with Objenesis
     * as a fallback when the default strategy cannot instantiate a type.
     */
    RecordKryoSerializerTest() {
        Kryo.DefaultInstantiatorStrategy strategy = new Kryo.DefaultInstantiatorStrategy();
        strategy.setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
        kryo.setInstantiatorStrategy(strategy);
    }
    /**
     * Verifies that a simple record without nested record fields can be serialized
     * and deserialized without changing its value.
     */
    @Test
    void roundTripsSimpleRecordWithNoNestedTypes() {
        kryo.register(AlertLeadTimeUpdate.class, serializer); // Use RecordKryoSerializer for AlertLeadTimeUpdate.
        AlertLeadTimeUpdate update = new AlertLeadTimeUpdate("INSULIN|MAIL_ORDER", 7);
        assertThat(roundTrip(update, AlertLeadTimeUpdate.class)).isEqualTo(update);
    }
    /**
     * Verifies that a record containing another record as a field can be
     * serialized and deserialized without changing its value.
     */
    @Test
    void roundTripsRecordWithNestedRecordField() {
        kryo.register(DrugClassRef.class, serializer);
        kryo.register(DrugClassRefUpdate.class, serializer);

        DrugClassRefUpdate update = new DrugClassRefUpdate("00069-4132-01",
                new DrugClassRef("INSULIN", true));
        assertThat(roundTrip(update, DrugClassRefUpdate.class)).isEqualTo(update);
    }
    /**
     * Verifies that a record containing a null field can be serialized and
     * deserialized without changing its value.
     */
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
    /**
     * Verifies that a record with a populated nullable field can be serialized
     * and deserialized without changing its value.
     *
     * <p><b>Populated nullable field:</b> A field that allows null values but
     * contains a value in this test case.
     */
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
    /**
     * Verifies that a record containing an immutable list of nested records can
     * be serialized and deserialized without changing its value.
     */
    @Test
    void roundTripsRecordWithListFieldBackedByImmutableList() {
        kryo.register(CoverageInterval.class, serializer);
        kryo.register(AdherenceState.class, serializer);

        AdherenceState state = new AdherenceState(
                LocalDate.of(2026, Month.JULY, 25),
                LocalDate.of(2026, Month.JULY, 20),
                5,
                List.of(new CoverageInterval("CLM-1",
                        LocalDate.of(2026, Month.JULY, 20),
                        LocalDate.of(2026, Month.JULY, 25))),
                10, null, null);

        assertThat(roundTrip(state, AdherenceState.class)).isEqualTo(state);
    }
    /**
     * Verifies that a record construction failure during deserialization is
     * wrapped in a {@link KryoException} with an appropriate error message.
     */
    @Test
    void readWrapsReflectiveFailureAsKryoException() {
        kryo.register(OddOnlyRecord.class, serializer);

        Output output = new Output(4096);
        kryo.writeClassAndObject(output, 4); // even -> OddOnlyRecord's compact constructor rejects it
        output.close();
        Input input = new Input(output.toBytes());
        // Suppress the unchecked cast warning; the record type is known to be a Record.
        @SuppressWarnings("unchecked")
        Class<Record> oddOnlyAsRecord = (Class<Record>) (Class<?>) OddOnlyRecord.class;

        assertThatThrownBy(() -> serializer.read(kryo, input, oddOnlyAsRecord))
                .isInstanceOf(KryoException.class)
                .hasMessageContaining("Failed to construct");
    }
    /**
     * Verifies that a record component accessor failure during serialization is
     * wrapped in a {@link KryoException} with an appropriate error message.
     */
    @Test
    void writeWrapsReflectiveFailureAsKryoException() {
        kryo.register(ThrowingAccessorRecord.class, serializer);

        Output output = new Output(4096);
        ThrowingAccessorRecord throwingAccessorRecord = new ThrowingAccessorRecord(1);

        assertThatThrownBy(() -> serializer.write(kryo, output, throwingAccessorRecord))
                .isInstanceOf(KryoException.class)
                .hasMessageContaining("Failed to read component");
    }
    /**
     * Serializes a value and deserializes it back using the configured Kryo serializer.
     *
     * <p>Used to verify that a record can successfully complete a serialization
     * round trip without loss of data.
     *
     * @param value the record to serialize and deserialize
     * @param type the runtime type of the record
     * @param <T> the record type
     * @return the deserialized record
     */
    private <T> T roundTrip(T value, Class<T> type) {
        Output output = new Output(4096);
        serializer.write(kryo, output, (Record) value);
        output.close();

        Input input = new Input(output.toBytes());
        @SuppressWarnings("unchecked")
        T result = (T) serializer.read(kryo, input, (Class<Record>) (Class<?>) type);
        return result;
    }
    /**
     * Test record with an accessor that intentionally throws an exception.
     *
     * <p>Used to verify that accessor failures during serialization are correctly
     * wrapped as {@link KryoException}.
     */
    private record ThrowingAccessorRecord(int value) {
        @Override
        public int value() {
            throw new RuntimeException("accessor boom");
        }
    }
    /**
     * Test record that rejects even values during construction.
     *
     * <p>Used to verify that constructor failures during deserialization are
     * correctly wrapped as {@link KryoException}.
     */
    private record OddOnlyRecord(int value) {
        OddOnlyRecord {
            if (value % 2 == 0) {
                throw new IllegalArgumentException("value must be odd");
            }
        }
    }
}
