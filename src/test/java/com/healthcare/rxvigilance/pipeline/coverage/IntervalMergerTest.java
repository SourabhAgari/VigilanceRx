package com.healthcare.rxvigilance.pipeline.coverage;

import com.healthcare.rxvigilance.domain.*;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.Month;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class IntervalMergerTest {

    private static RxFillEvent fillEvent(String claimId, LocalDate fillDate, int daySupply) {
        return new RxFillEvent(
                EventType.FILL, claimId, "MBR-1", "NDC-1", fillDate, daySupply,
                BigDecimal.valueOf(30), "PHM-1", "RX-1", 3, Channel.RETAIL, null);
    }

    private static AdherenceState emptyState() {
        return new AdherenceState(null, null, 0, List.of(), 5, null);
    }

    @Test
    void nonOverlappingFillsAppendsClearly() {
        AdherenceState afterFirst = IntervalMerger.merge(emptyState(),
                fillEvent("CLM1-1", LocalDate.of(2026, Month.JANUARY, 1), 30));
        AdherenceState afterSecond = IntervalMerger.merge(afterFirst,
                fillEvent("CLM-2", LocalDate.of(2026, Month.FEBRUARY, 15), 30));

        assertThat(afterSecond.currentSupplyEndDate()).isEqualTo(LocalDate.of(2026, Month.MARCH, 17));
        assertThat(afterSecond.totalDaysCovered()).isEqualTo(60);
        assertThat(afterSecond.activeCoverageIntervals()).hasSize(2);
    }

    @Test
    void earlyRefillOnlyNonOverlappingDaysCount() {
        AdherenceState afterFirst = IntervalMerger.merge(emptyState(),
                fillEvent("CLM-1", LocalDate.of(2026, Month.JANUARY, 1), 30));
        AdherenceState afterEarlyRefill = IntervalMerger.merge(afterFirst,
                fillEvent("CLM-2", LocalDate.of(2026, Month.JANUARY, 25), 30));

        assertThat(afterEarlyRefill.currentSupplyEndDate()).isEqualTo(LocalDate.of(2026, Month.FEBRUARY, 24));
        assertThat(afterEarlyRefill.totalDaysCovered()).isEqualTo(54);
    }

    @Test
    void fillFullyInsideExistingCoverageAddsZeroDays() {
        AdherenceState afterFirst = IntervalMerger.merge(emptyState(),
                fillEvent("CLM-1", LocalDate.of(2026, Month.JANUARY, 1), 60));

        AdherenceState afterInsideFill = IntervalMerger.merge(afterFirst,
                fillEvent("CLM-2", LocalDate.of(2026, Month.JANUARY, 10), 10));

        assertThat(afterInsideFill.currentSupplyEndDate()).isEqualTo(LocalDate.of(2026, Month.MARCH, 2));
        assertThat(afterInsideFill.totalDaysCovered()).isEqualTo(60);
        assertThat(afterInsideFill.activeCoverageIntervals()).hasSize(2);
    }

    @Test
    void outOfOrderFillMergesCorrectly() {
        AdherenceState state = new AdherenceState(
                LocalDate.of(2026, Month.MARCH, 3), LocalDate.of(2026, Month.FEBRUARY, 1), 30,
                List.of(new CoverageInterval("LATER-1", LocalDate.of(2026, Month.FEBRUARY, 1), LocalDate.of(2026, Month.MARCH, 3))),
                5, null);

        AdherenceState afterOlderFill = IntervalMerger.merge(state,
                fillEvent("OLDER-1", LocalDate.of(2026, Month.JANUARY, 1), 15));

        assertThat(afterOlderFill.currentSupplyEndDate()).isEqualTo(LocalDate.of(2026, Month.MARCH, 3));
        assertThat(afterOlderFill.totalDaysCovered()).isEqualTo(45);
        assertThat(afterOlderFill.activeCoverageIntervals()).hasSize(2);
        assertThat(afterOlderFill.lastFillDate()).isEqualTo(LocalDate.of(2026, Month.FEBRUARY, 1));
    }

    @Test
    void reversalOfLatestFillShrinksEndDate() {
        AdherenceState afterFirst = IntervalMerger.merge(emptyState(),
                fillEvent("CLM-1", LocalDate.of(2026, Month.JANUARY, 1), 30));
        AdherenceState afterSecond = IntervalMerger.merge(afterFirst,
                fillEvent("CLM-2", LocalDate.of(2026, Month.FEBRUARY, 15), 30));

        AdherenceState afterReversal = IntervalMerger.unwind(afterSecond, "CLM-2");
        assertThat(afterReversal.currentSupplyEndDate()).isEqualTo(LocalDate.of(2026, Month.JANUARY, 31));
        assertThat(afterReversal.totalDaysCovered()).isEqualTo(30);
        assertThat(afterReversal.activeCoverageIntervals()).hasSize(1);
    }

    @Test
    void reversalOfMiddleIntervalRecomputesCorrectly() {
        AdherenceState state = IntervalMerger.merge(emptyState(),
                fillEvent("CLM-1", LocalDate.of(2026, Month.JANUARY, 1), 30));
        state = IntervalMerger.merge(state,
                fillEvent("CLM-2", LocalDate.of(2026, Month.FEBRUARY, 15), 30));
        state = IntervalMerger.merge(state,
                fillEvent("CLM-3", LocalDate.of(2026, Month.APRIL, 1), 30));

        AdherenceState afterReversal = IntervalMerger.unwind(state, "CLM-2");

        assertThat(afterReversal.currentSupplyEndDate()).isEqualTo(LocalDate.of(2026, Month.MAY, 1));
        assertThat(afterReversal.totalDaysCovered()).isEqualTo(60);
        assertThat(afterReversal.activeCoverageIntervals()).hasSize(2);
    }

    @Test
    void reversalOfUnknownClaimIdIsSafeNoOp() {
        AdherenceState state = IntervalMerger.merge(emptyState(),
                fillEvent("CLM-1", LocalDate.of(2026, Month.JANUARY, 1), 30));

        AdherenceState afterReversal = IntervalMerger.unwind(state, "NEVER-EXISTED");

        assertThat(afterReversal).isSameAs(state);
    }

    @Test
    void reversalLeavingZeroCoverageReturnsEmptyStateSignal() {
        AdherenceState state = IntervalMerger.merge(emptyState(),
                fillEvent("CLM-1", LocalDate.of(2026, Month.JANUARY, 1), 30));

        AdherenceState afterReversal = IntervalMerger.unwind(state, "CLM-1");

        assertThat(afterReversal.currentSupplyEndDate()).isNull();
        assertThat(afterReversal.totalDaysCovered()).isZero();
        assertThat(afterReversal.activeCoverageIntervals()).isEmpty();
    }

    @Test
    void duplicateClaimIdFillIsIdempotentNoOp() {
        AdherenceState state = IntervalMerger.merge(emptyState(),
                fillEvent("CLM-1", LocalDate.of(2026, Month.JANUARY, 1), 30));

        AdherenceState afterDuplicate = IntervalMerger.merge(state,
                fillEvent("CLM-1", LocalDate.of(2026, Month.JANUARY, 1), 30));

        assertThat(afterDuplicate).isSameAs(state);
    }


}
