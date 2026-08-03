package com.healthcare.rxvigilance.metrics;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

public class AdherenceMetricsReporter {

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
