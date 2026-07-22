package com.healthcare.rxvigilance.domain;

import java.math.BigDecimal;
import java.time.LocalDate;

public record RxFillEvent(
        EventType eventType,
        String claimId,
        String memberId,
        String ndcCode,
        LocalDate fillDate,
        int daySupply,
        BigDecimal quantity,
        String pharmacyId,
        String rxNumber,
        int refillsAuthorized,
        Channel dispensingChanel,
        String originalClaimId
) { }
