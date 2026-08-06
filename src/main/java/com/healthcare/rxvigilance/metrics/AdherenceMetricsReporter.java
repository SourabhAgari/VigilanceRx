package com.healthcare.rxvigilance.metrics;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

public class AdherenceMetricsReporter {

    /**
     * Metric names are a published contract: Phase 11 dashboards and alerts key on these exact
     * strings, so a rename here silently empties a panel rather than failing a build. Frozen by
     * AdherenceMetricsReporterTest, the same way §4's broadcast descriptor names are frozen.
     */
    public static final String METRIC_GROUP = "adherence";
    public static final String CHRONIC_FILTER_DROPPED = "chronicFilterDropped";
    public static final String MISSING_LEAD_TIME_LOOKUP = "missingLeadTimeLookup";
    public static final String GAP_RISK_ALERTS_EMITTED = "gapRiskAlertsEmitted";
    public static final String LAPSED_ALERTS_EMITTED = "lapsedAlertsEmitted";

    private final Counter chronicFilterDropped;
    private final Counter missingLeadTimeLookup;
    private final Counter gapRiskAlertsEmitted;
    private final Counter lapsedAlertsEmitted;

    private AdherenceMetricsReporter(MetricGroup group) {
        this.chronicFilterDropped = group.counter("chronicFilterDropped");
        this.missingLeadTimeLookup = group.counter("missingLeadTimeLookup");
        this.gapRiskAlertsEmitted = group.counter("gapRiskAlertsEmitted");
        this.lapsedAlertsEmitted = group.counter("lapsedAlertsEmitted");
    }

    public static AdherenceMetricsReporter register(RuntimeContext runtimeContext) {
        return new AdherenceMetricsReporter(runtimeContext.getMetricGroup().addGroup("adherence"));
    }

    public Counter chronicFilterDropped() {
        return chronicFilterDropped;
    }

    public Counter missingLeadTimeLookup() {
        return missingLeadTimeLookup;
    }

    public Counter gapRiskAlertsEmitted() {
        return gapRiskAlertsEmitted;
    }

    public Counter lapsedAlertsEmitted() {
        return lapsedAlertsEmitted;
    }
}
