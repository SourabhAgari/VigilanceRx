package com.healthcare.rxvigilance.domain;

import java.time.LocalDate;

public record PdcSnapshot(
        String memberId,
        String drugClass,
        int totalDaysCovered,
        LocalDate currentSupplyEndDate,
        long emittedAt
) { }
