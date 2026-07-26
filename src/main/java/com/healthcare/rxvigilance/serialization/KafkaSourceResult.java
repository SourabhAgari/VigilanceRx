package com.healthcare.rxvigilance.serialization;

import java.util.Arrays;
import java.util.Objects;

public record KafkaSourceResult<T>(T value, byte[] rawBytes, String errorMessage) {
    public boolean isSuccess() {
        return value != null;
    }

    public static <T> KafkaSourceResult<T> success(T value) {
        return new KafkaSourceResult<>(value, null, null);
    }

    public static <T> KafkaSourceResult<T> failure(byte[] rawBytes, String errorMessage) {
        return new KafkaSourceResult<>(null, rawBytes, errorMessage);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaSourceResult<?> other)) {
            return false;
        }
        return Objects.equals(value, other.value)
                && Arrays.equals(rawBytes, other.rawBytes)
                && Objects.equals(errorMessage, other.errorMessage);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(value, errorMessage);
        result = 31 * result + Arrays.hashCode(rawBytes);
        return result;
    }

    @Override
    public String toString() {
        return "KafkaSourceResult[value=" + value
                + ", rawBytes=" + Arrays.toString(rawBytes)
                + ", errorMessage=" + errorMessage + "]";
    }
}
