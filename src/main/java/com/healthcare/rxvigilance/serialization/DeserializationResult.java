package com.healthcare.rxvigilance.serialization;

import com.healthcare.rxvigilance.domain.RxFillEvent;

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
}