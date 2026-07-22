package com.healthcare.rxvigilance.domain;

import java.time.LocalDate;
import java.util.List;

public record AdherenceState(LocalDate currentSupplyDate, LocalDate lastFillDate, int totalDaysCovered,
                             List<CoverageInterval> activeCoverageIntervals, int alertLeadDays,
                             Long activeTimerTimestamp) {
    public AdherenceState {
        activeCoverageIntervals = List.copyOf(activeCoverageIntervals);
    }
}
