package com.healthcare.rxvigilance.metrics;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import java.util.HashMap;
import java.util.Map;

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
    public static final String DEAD_LETTER_RECORDS = "deadLetterRecords";
    public static final String DUPLICATE_CLAIM_ID_DROPPED = "duplicateClaimIdDropped";
    public static final String REVERSAL_WITHOUT_ORIGINAL = "reversalWithoutOriginal";
    public static final String TIMERS_REGISTERED = "timersRegistered";
    public static final String TIMERS_FIRED = "timersFired";
    public static final String PDC_SNAPSHOTS_EMITTED = "pdcSnapshotsEmitted";
    public static final String BROADCAST_ENTRIES_LOADED = "broadcastEntriesLoaded";

    private final MetricGroup group;

    /**
     * Metrics are created on first request, not all up front (#150). Registering the whole set
     * on every operator would publish a permanently-zero series for each metric that operator
     * does not touch — eleven metrics across five operators, most of them noise a dashboard
     * then has to filter out. The map also makes a repeated call idempotent: Flink's MetricGroup
     * rejects a second registration of the same name and returns a counter nothing reports, so
     * without it a duplicated accessor call would increment into a void.
     */
    private final Map<String, Counter> counters = new HashMap<>();
    private final Map<String, Gauge<Long>> gauges = new HashMap<>();

    private AdherenceMetricsReporter(MetricGroup group) {
        this.group = group;
    }

    public static AdherenceMetricsReporter register(RuntimeContext runtimeContext) {
        return new AdherenceMetricsReporter(
                runtimeContext.getMetricGroup().addGroup(METRIC_GROUP));
    }

    private Counter counter(String name) {
        return counters.computeIfAbsent(name, metricName -> group.counter(metricName));
    }

    public Counter chronicFilterDropped() {
        return counter(CHRONIC_FILTER_DROPPED);
    }

    public Counter missingLeadTimeLookup() {
        return counter(MISSING_LEAD_TIME_LOOKUP);
    }

    public Counter gapRiskAlertsEmitted() {
        return counter(GAP_RISK_ALERTS_EMITTED);
    }

    public Counter lapsedAlertsEmitted() {
        return counter(LAPSED_ALERTS_EMITTED);
    }

    public Counter deadLetterRecords() {
        return counter(DEAD_LETTER_RECORDS);
    }

    public Counter duplicateClaimIdDropped() {
        return counter(DUPLICATE_CLAIM_ID_DROPPED);
    }

    public Counter reversalWithoutOriginal() {
        return counter(REVERSAL_WITHOUT_ORIGINAL);
    }

    public Counter timersRegistered() {
        return counter(TIMERS_REGISTERED);
    }

    public Counter timersFired() {
        return counter(TIMERS_FIRED);
    }

    public Counter pdcSnapshotsEmitted() {
        return counter(PDC_SNAPSHOTS_EMITTED);
    }

    /**
     * A gauge is read by the reporter thread, not the task thread, so it is handed the supplier
     * rather than a value. One name serves both broadcasts: they live on different operators, so
     * Flink's operator_name label already separates them and no per-broadcast name is needed.
     */
    public void broadcastEntriesLoaded(Gauge<Long> gauge) {
        gauges.put(BROADCAST_ENTRIES_LOADED, gauge);
        group.gauge(BROADCAST_ENTRIES_LOADED, gauge);
    }

    /**
     * Reads a counter that has already been registered. Throws rather than returning zero for an
     * unknown name: a typo in a test assertion would otherwise read as "the counter never moved"
     * and pass green, which is the failure this method exists to catch.
     */
    public long count(String name) {
        Counter counter = counters.get(name);
        if (counter == null) {
            throw new IllegalArgumentException("No counter registered under name: " + name);
        }
        return counter.getCount();
    }

    /** Gauge counterpart to {@link #count}, with the same throw-on-unknown-name behaviour. */
    public long gaugeValue(String name) {
        Gauge<Long> gauge = gauges.get(name);
        if (gauge == null) {
            throw new IllegalArgumentException("No gauge registered under name: " + name);
        }
        return gauge.getValue();
    }
}