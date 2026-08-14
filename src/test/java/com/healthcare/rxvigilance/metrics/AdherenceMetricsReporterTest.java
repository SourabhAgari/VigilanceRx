package com.healthcare.rxvigilance.metrics;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AdherenceMetricsReporterTest {

    /**
     * Freezes the metric-name contract consumed by Phase 11 dashboards. Renaming a counter is a
     * breaking change to something outside this repo, exactly like renaming a broadcast state
     * descriptor — the test exists so the rename fails here rather than in Grafana.
     */
    @Test
    void metricNamesAreFrozen() {
        assertThat(AdherenceMetricsReporter.METRIC_GROUP).isEqualTo("adherence");
        assertThat(AdherenceMetricsReporter.CHRONIC_FILTER_DROPPED).isEqualTo("chronicFilterDropped");
        assertThat(AdherenceMetricsReporter.MISSING_LEAD_TIME_LOOKUP).isEqualTo("missingLeadTimeLookup");
        assertThat(AdherenceMetricsReporter.GAP_RISK_ALERTS_EMITTED).isEqualTo("gapRiskAlertsEmitted");
        assertThat(AdherenceMetricsReporter.LAPSED_ALERTS_EMITTED).isEqualTo("lapsedAlertsEmitted");
        assertThat(AdherenceMetricsReporter.DEAD_LETTER_RECORDS).isEqualTo("deadLetterRecords");
        assertThat(AdherenceMetricsReporter.DUPLICATE_CLAIM_ID_DROPPED).isEqualTo("duplicateClaimIdDropped");
        assertThat(AdherenceMetricsReporter.REVERSAL_WITHOUT_ORIGINAL).isEqualTo("reversalWithoutOriginal");
        assertThat(AdherenceMetricsReporter.TIMERS_REGISTERED).isEqualTo("timersRegistered");
        assertThat(AdherenceMetricsReporter.TIMERS_FIRED).isEqualTo("timersFired");
        assertThat(AdherenceMetricsReporter.PDC_SNAPSHOTS_EMITTED).isEqualTo("pdcSnapshotsEmitted");
        assertThat(AdherenceMetricsReporter.BROADCAST_ENTRIES_LOADED).isEqualTo("broadcastEntriesLoaded");
    }

    /**
     * Two constants holding the same string would collide at registration: Flink logs a name
     * collision and the second metric is never reported, so one counter would silently vanish
     * from every dashboard. A copy-paste of a constant is exactly how that happens.
     */
    @Test
    void metricNamesAreDistinct() {
        assertThat(List.of(
                AdherenceMetricsReporter.CHRONIC_FILTER_DROPPED,
                AdherenceMetricsReporter.MISSING_LEAD_TIME_LOOKUP,
                AdherenceMetricsReporter.GAP_RISK_ALERTS_EMITTED,
                AdherenceMetricsReporter.LAPSED_ALERTS_EMITTED,
                AdherenceMetricsReporter.DEAD_LETTER_RECORDS,
                AdherenceMetricsReporter.DUPLICATE_CLAIM_ID_DROPPED,
                AdherenceMetricsReporter.REVERSAL_WITHOUT_ORIGINAL,
                AdherenceMetricsReporter.TIMERS_REGISTERED,
                AdherenceMetricsReporter.TIMERS_FIRED,
                AdherenceMetricsReporter.PDC_SNAPSHOTS_EMITTED,
                AdherenceMetricsReporter.BROADCAST_ENTRIES_LOADED))
                .doesNotHaveDuplicates();
    }
}
