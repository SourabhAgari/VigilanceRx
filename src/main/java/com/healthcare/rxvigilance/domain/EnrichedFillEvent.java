package com.healthcare.rxvigilance.domain;

public record EnrichedFillEvent(RxFillEvent event, String drugClass) { }
