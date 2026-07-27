package com.healthcare.rxvigilance.serialization.util;

import com.healthcare.rxvigilance.serialization.KafkaSourceResult;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DeadLetterRecordTest {
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
        DeadLetterRecord record = new DeadLetterRecord(new byte[]{1, 2, 3}, "err");

        assertThat(record).isEqualTo(record);
    }

    @Test
    void equalsReturnsFalseForDifferentType() {
        DeadLetterRecord record = new DeadLetterRecord(new byte[]{1, 2, 3}, "err");

        assertThat(record.equals("not a DeadLetterRecord")).isFalse();
    }

    @Test
    void equalsAndHashCodeCompareRawBytesByContentNotReference() {
        DeadLetterRecord first = new DeadLetterRecord(new byte[]{1, 2, 3}, "err");
        DeadLetterRecord second = new DeadLetterRecord(new byte[]{1, 2, 3}, "err");

        assertThat(first).isEqualTo(second);
        assertThat(first.hashCode()).isEqualTo(second.hashCode());
    }

    @Test
    void toStringIncludesRawBytesAndErrorMessage() {
        DeadLetterRecord record = new DeadLetterRecord(new byte[]{1, 2, 3}, "bad bytes");

        assertThat(record.toString())
                .contains("bad bytes")
                .contains("[1, 2, 3]");
    }
}
