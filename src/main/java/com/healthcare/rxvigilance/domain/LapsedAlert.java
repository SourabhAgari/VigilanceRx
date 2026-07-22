package com.healthcare.rxvigilance.domain;

import java.time.LocalDate;

public record LapsedAlert(
        String alertId,
        String memberId,
        String drugClass,
        LocalDate lapsedOn,
        long emittedAt
) { }
