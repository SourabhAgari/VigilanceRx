package com.healthcare.rxvigilance.pipeline.coverage;

import com.healthcare.rxvigilance.domain.AdherenceState;
import com.healthcare.rxvigilance.domain.Channel;
import com.healthcare.rxvigilance.domain.EventType;
import com.healthcare.rxvigilance.domain.RxFillEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.Month;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class IntervalMergerTest {

    private static RxFillEvent fillEvent(String claimId, LocalDate fillDate, int daySupply) {
        return new RxFillEvent(
                EventType.FILL, claimId, "MBR-1", "NDC-1", fillDate, daySupply,
                BigDecimal.valueOf(30), "PHM-1", "RX-1", 3, Channel.RETAIL, null);
    }

    private static AdherenceState emptyState() {
        return new AdherenceState(null,null,0, List.of(),5,null);
    }

    @Test
    void nonOverlappingFillsAppendsClearly() {
        AdherenceState afterFirst = IntervalMerger.merge(emptyState(),
                fillEvent("CLM1-1",LocalDate.of(2026, Month.JANUARY,1),30));
        AdherenceState afterSecond = IntervalMerger.merge(afterFirst,
                fillEvent("CLM-2", LocalDate.of(2026, Month.FEBRUARY, 15), 30));

        assertThat(afterSecond.currentSupplyEndDate()).isEqualTo(LocalDate.of(2026,Month.MARCH,17));
        assertThat(afterSecond.totalDaysCovered()).isEqualTo(60);
        assertThat(afterSecond.activeCoverageIntervals()).hasSize(2);
    }


}
