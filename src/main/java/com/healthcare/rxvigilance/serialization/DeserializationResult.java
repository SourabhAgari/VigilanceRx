package com.healthcare.rxvigilance.serialization;

import com.healthcare.rxvigilance.domain.RxFillEvent;

import java.util.Arrays;
import java.util.Objects;

public record DeserializationResult(RxFillEvent event, byte[] rawBytes, String errorMessage) {

    public boolean isSuccess() {
        return event != null;
    }

    public static DeserializationResult success(RxFillEvent event) {
        return new DeserializationResult(event, null, null);
    }

    public static DeserializationResult failure(byte[] rawBytes, String errorMessage) {
        return new DeserializationResult(null, rawBytes, errorMessage);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DeserializationResult other)) {
            return false;
        }
        return Objects.equals(event, other.event)
                && Arrays.equals(rawBytes, other.rawBytes)
                && Objects.equals(errorMessage, other.errorMessage);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(event, errorMessage);
        result = 31 * result + Arrays.hashCode(rawBytes);
        return result;
    }

    @Override
    public String toString() {
        return "DeserializationResult[event=" + event
                + ", rawBytes=" + Arrays.toString(rawBytes)
                + ", errorMessage=" + errorMessage + "]";
    }
}