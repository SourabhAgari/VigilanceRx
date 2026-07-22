package com.healthcare.rxvigilance.domain;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.Month;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AdherenceStateTest {

    @Test
    void mutatingOriginalListDoesNotAffectStoredState() {
        List<CoverageInterval> original = new ArrayList<>();
        original.add(new CoverageInterval("CLM-1",
                LocalDate.of(2026, Month.JANUARY, 1),
                LocalDate.of(2026, Month.JANUARY, 31)));

        AdherenceState adherenceState = new AdherenceState(
                LocalDate.of(2026, Month.JANUARY, 31),
                LocalDate.of(2026, Month.JANUARY, 30), 30, original, 5, null);

        original.add(new CoverageInterval("CLM-2",
                LocalDate.of(2026, Month.FEBRUARY, 1),
                LocalDate.of(2026, Month.FEBRUARY, 28)));

        assertThat(adherenceState.activeCoverageIntervals()).hasSize(1);
    }

    @Test
    void returnedIntervalListIsUnmodifiable() {
        AdherenceState state = new AdherenceState(
                LocalDate.of(2026, Month.JANUARY, 31),
                LocalDate.of(2026, Month.JANUARY, 1), 30, List.of(), 5, null);

        CoverageInterval interval = new CoverageInterval(
                "CLM-1",
                LocalDate.of(2026, Month.JANUARY, 1),
                LocalDate.of(2026, Month.JANUARY, 2));

        List<CoverageInterval> intervals = state.activeCoverageIntervals();

        assertThatThrownBy(() -> intervals.add(interval))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
