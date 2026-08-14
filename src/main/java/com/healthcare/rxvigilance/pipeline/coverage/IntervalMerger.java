package com.healthcare.rxvigilance.pipeline.coverage;

import com.healthcare.rxvigilance.domain.AdherenceState;
import com.healthcare.rxvigilance.domain.CoverageInterval;
import com.healthcare.rxvigilance.domain.RxFillEvent;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public final class IntervalMerger {
    private IntervalMerger() {
    }

    public record CoverageSummary(LocalDate currentSupplyEndDate, int totalDaysCovered) {
    }

    /** What {@link #merge} did. Kept separate from {@link UnwindOutcome} so merge cannot report a reversal reason. */
    public enum MergeOutcome {
        APPLIED,
        DUPLICATE_CLAIM
    }

    /** What {@link #unwind} did. */
    public enum UnwindOutcome {
        APPLIED,
        NO_MATCHING_INTERVAL
    }

    public record MergeResult(AdherenceState state, MergeOutcome outcome) {
    }

    public record UnwindResult(AdherenceState state, UnwindOutcome outcome) {
    }

    public static CoverageSummary recompute(List<CoverageInterval> intervals) {
        if (intervals.isEmpty()) {
            return new CoverageSummary(null, 0);
        }

        List<CoverageInterval> sorted = intervals
                .stream().sorted(Comparator.comparing(CoverageInterval::start))
                .toList();

        LocalDate currentSupplyEndDate = sorted.get(0).end();
        LocalDate mergedStart = sorted.get(0).start();
        LocalDate mergedEnd = sorted.get(0).end();
        int totalDaysCovered = (int) ChronoUnit.DAYS.between(mergedStart, mergedEnd);

        for (int i = 1; i < sorted.size(); i++) {
            CoverageInterval next = sorted.get(i);
            boolean overlapsOrAdjacent = !next.start().isAfter(mergedEnd);

            if (overlapsOrAdjacent && next.end().isAfter(mergedEnd)) {
                totalDaysCovered += (int) ChronoUnit.DAYS.between(mergedEnd, next.end());
                mergedEnd = next.end();
            } else if (!overlapsOrAdjacent) {
                totalDaysCovered += (int) ChronoUnit.DAYS.between(next.start(), next.end());
                mergedEnd = next.end();
            }

            if (next.end().isAfter(currentSupplyEndDate)) {
                currentSupplyEndDate = next.end();
            }
        }
        return new CoverageSummary(currentSupplyEndDate, totalDaysCovered);
    }

    public static MergeResult merge(AdherenceState state, RxFillEvent fill) {
        boolean alreadyProcessed = state.activeCoverageIntervals().stream()
                .anyMatch(interval -> interval.claimId().equals(fill.claimId()));
        if (alreadyProcessed) {
            return new MergeResult(state, MergeOutcome.DUPLICATE_CLAIM);
        }

        CoverageInterval newInterval = new CoverageInterval(
                fill.claimId(), fill.fillDate(), fill.fillDate().plusDays(fill.daySupply())
        );

        List<CoverageInterval> updatedIntervals = new ArrayList<>(state.activeCoverageIntervals());
        updatedIntervals.add(newInterval);

        CoverageSummary summary = recompute(updatedIntervals);

        LocalDate lastFillDate = (state.lastFillDate() == null || fill.fillDate().isAfter(state.lastFillDate()))
                ? fill.fillDate()
                : state.lastFillDate();

        return new MergeResult(new AdherenceState(
                summary.currentSupplyEndDate(),
                lastFillDate,
                summary.totalDaysCovered(),
                updatedIntervals,
                state.alertLeadDays(),
                state.activeTimerTimestamp(),
                state.activeTimerStage()), MergeOutcome.APPLIED);
    }


    public static UnwindResult unwind(AdherenceState state, String originalClaimId) {
        List<CoverageInterval> remaining = new ArrayList<>(state.activeCoverageIntervals().stream()
                .filter(interval -> !interval.claimId().equals(originalClaimId))
                .toList());

        if (remaining.size() == state.activeCoverageIntervals().size()) {
            return new UnwindResult(state, UnwindOutcome.NO_MATCHING_INTERVAL);
        }

        CoverageSummary summary = recompute(remaining);

        return new UnwindResult(new AdherenceState(
                summary.currentSupplyEndDate(),
                state.lastFillDate(),
                summary.totalDaysCovered(),
                remaining,
                state.alertLeadDays(),
                state.activeTimerTimestamp(),
                state.activeTimerStage()), UnwindOutcome.APPLIED);
    }
}
