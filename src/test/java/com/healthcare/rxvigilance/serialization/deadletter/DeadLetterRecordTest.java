package com.healthcare.rxvigilance.serialization.deadletter;

import com.healthcare.rxvigilance.serialization.util.KafkaCoordinates;
import com.healthcare.rxvigilance.serialization.util.KafkaSourceResult;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DeadLetterRecordTest {

    private static final KafkaCoordinates COORDS =
            new KafkaCoordinates("rx-fill-events", 2, 884211L, 1_700_000_000_000L);

    @Test
    void fromExtractsRawBytesAndErrorMessageRegardlessOfSourceType() {
        byte[] rawBytes = {1, 2, 3};
        KafkaSourceResult<String> stringFailure = KafkaSourceResult.failure(rawBytes, "bad string");
        KafkaSourceResult<Integer> intFailure = KafkaSourceResult.failure(rawBytes, "bad string");

        DeadLetterRecord fromString = DeadLetterRecord.from(stringFailure);
        DeadLetterRecord fromInt = DeadLetterRecord.from(intFailure);

        assertThat(fromString.rawBytes()).isEqualTo(rawBytes);
        assertThat(fromString.errorMessage()).isEqualTo("bad string");
        assertThat(fromString).isEqualTo(fromInt);
    }

    @Test
    void equalsIsReflexive() {
        DeadLetterRecord deadLetterRecord = new DeadLetterRecord(new byte[]{1, 2, 3}, "err",COORDS);

        assertThat(deadLetterRecord).isEqualTo(deadLetterRecord);
    }

    @Test
    void equalsReturnsFalseForDifferentType() {
        DeadLetterRecord deadLetterRecord = new DeadLetterRecord(new byte[]{1, 2, 3}, "err",COORDS);

        assertThat(deadLetterRecord.equals("not a DeadLetterRecord")).isFalse();
    }

    @Test
    void equalsAndHashCodeCompareRawBytesByContentNotReference() {
        DeadLetterRecord first = new DeadLetterRecord(new byte[]{1, 2, 3}, "err",COORDS);
        DeadLetterRecord second = new DeadLetterRecord(new byte[]{1, 2, 3}, "err",COORDS);

        assertThat(first).isEqualTo(second);
        assertThat(first.hashCode()).hasSameHashCodeAs(second.hashCode());
    }

    /**
     * The doesNotContain is the actual guard, not the comment on toString: raw bytes
     * are PHI (§9) and #148 will log this object. #149.
     */
    @Test
    void toStringReportsRawBytesLengthAndCoordinatesButNeverContent() {
        DeadLetterRecord deadLetterRecord =
                new DeadLetterRecord(new byte[]{1, 2, 3}, "bad bytes", COORDS);

        assertThat(deadLetterRecord.toString())
                .contains("bad bytes")
                .contains("rawBytesLength=3")
                .contains("rx-fill-events")
                .contains("884211")
                .doesNotContain("[1, 2, 3]");
    }
    @Test
    void equalsDistinguishesRecordsFromDifferentBrokerPositions() {
        DeadLetterRecord first = new DeadLetterRecord(new byte[]{1, 2, 3}, "err", COORDS);
        DeadLetterRecord second = new DeadLetterRecord(new byte[]{1, 2, 3}, "err",
                new KafkaCoordinates("rx-fill-events", 5, 12L, 1_700_000_000_000L));

        assertThat(first).isNotEqualTo(second);
    }
}
