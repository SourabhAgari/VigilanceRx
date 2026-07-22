package com.healthcare.rxvigilance.domain;

import java.time.LocalDate;

public record GapRiskAlert(
        String alertId,
        String memberId, String drugClass,
        LocalDate expiresOn,
        int leadDays,
        long emittedAt
) { }
