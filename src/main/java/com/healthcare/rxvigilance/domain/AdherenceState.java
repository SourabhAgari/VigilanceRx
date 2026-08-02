package com.healthcare.rxvigilance.domain;

import com.healthcare.rxvigilance.domain.enums.TimerStage;

import java.time.LocalDate;
import java.util.List;

public record AdherenceState(LocalDate currentSupplyEndDate, LocalDate lastFillDate, int totalDaysCovered,
                             List<CoverageInterval> activeCoverageIntervals, int alertLeadDays,
                             Long activeTimerTimestamp, TimerStage activeTimerStage) {
    public AdherenceState {
        activeCoverageIntervals = List.copyOf(activeCoverageIntervals);
    }
}
