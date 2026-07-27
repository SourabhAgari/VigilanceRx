package com.healthcare.rxvigilance.serialization.util;

import com.healthcare.rxvigilance.serialization.KafkaSourceResult;

import java.util.Arrays;
import java.util.Objects;

public record DeadLetterRecord(byte[] rawBytes, String errorMessage) {

    public static DeadLetterRecord from(KafkaSourceResult<?> result) {
        return new DeadLetterRecord(result.rawBytes(), result.errorMessage());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DeadLetterRecord other)) {
            return false;
        }
        return Arrays.equals(rawBytes, other.rawBytes)
                && Objects.equals(errorMessage, other.errorMessage);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(errorMessage);
        result = 31 * result + Arrays.hashCode(rawBytes);
        return result;
    }

    @Override
    public String toString() {
        return "DeadLetterRecord[rawBytes=" + Arrays.toString(rawBytes)
                + ", errorMessage=" + errorMessage + "]";
    }
}