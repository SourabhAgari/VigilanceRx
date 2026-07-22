package com.healthcare.rxvigilance.domain;

import java.time.LocalDate;

public record CoverageInterval(String claimId, LocalDate start,LocalDate end) {
    // customizing the constructor behavior
    public CoverageInterval {
        if (start.isAfter(end)) {
            throw new IllegalArgumentException(
                    "CoverageInterval start (" + start + ") is after end (" + end + ") for claimId " + claimId);
        }
    }
}
