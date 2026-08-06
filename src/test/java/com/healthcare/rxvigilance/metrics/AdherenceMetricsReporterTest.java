package com.healthcare.rxvigilance.metrics;

import org.junit.jupiter.api.Test;

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
    }
}
