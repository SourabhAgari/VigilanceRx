package com.healthcare.rxvigilance.domain;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.Month;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CoverageIntervalTest {

    @Test
    void constructsWhenStartIsBeforeEnd() {
        LocalDate start = LocalDate.of(2026, Month.JANUARY, 1);
        LocalDate end = LocalDate.of(2026, Month.JANUARY, 31);

        CoverageInterval interval = new CoverageInterval(
                "CLM-001", start, end);
                assertThat(interval.start()).isEqualTo(LocalDate.of(2026,Month.JANUARY,1));
                assertThat(interval.end()).isEqualTo(LocalDate.of(2026,Month.JANUARY,31));
    }

    @Test
    void rejectsStartAfterEnd() {
        LocalDate start = LocalDate.of(2026, Month.JANUARY, 1);
        LocalDate end = LocalDate.of(2025, Month.JANUARY, 31);

        assertThatThrownBy(() -> new CoverageInterval("CLM-001", start, end))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
