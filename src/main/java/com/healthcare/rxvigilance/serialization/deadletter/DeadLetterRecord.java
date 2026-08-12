package com.healthcare.rxvigilance.serialization.deadletter;

import com.healthcare.rxvigilance.serialization.util.KafkaCoordinates;
import com.healthcare.rxvigilance.serialization.util.KafkaSourceResult;

import java.util.Arrays;
import java.util.Objects;

public record DeadLetterRecord(byte[] rawBytes, String errorMessage, KafkaCoordinates coordinates) {

    public static DeadLetterRecord from(KafkaSourceResult<?> result) {
        return new DeadLetterRecord(result.rawBytes(), result.errorMessage(),result.coordinates());
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
                && Objects.equals(errorMessage, other.errorMessage)
                && Objects.equals(coordinates, other.coordinates);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(errorMessage,coordinates);
        result = 31 * result + Arrays.hashCode(rawBytes);
        return result;
    }

    /**
     * rawBytes are a member's full record — PHI under §9 — and #148's dead-letter WARN
     * will log this object. Printing the payload would put PHI into Cloud Logging, where
     * the dead-letter topic's ACLs no longer protect it. The length distinguishes a
     * truncated message from an empty one; the coordinates say where to read the real
     * thing. #149.
     */
    @Override
    public String toString() {
        return "DeadLetterRecord[rawBytesLength=" + (rawBytes == null ? 0 : rawBytes.length)
                + ", errorMessage=" + errorMessage
                + ", coordinates=" + coordinates + "]";
    }
}