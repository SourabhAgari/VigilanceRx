package com.healthcare.rxvigilance.pipeline.operators;

import com.healthcare.rxvigilance.config.StateBackEndConfig;
import com.healthcare.rxvigilance.domain.*;
import com.healthcare.rxvigilance.domain.enums.EventType;
import com.healthcare.rxvigilance.domain.enums.TimerStage;
import com.healthcare.rxvigilance.metrics.AdherenceMetricsReporter;
import com.healthcare.rxvigilance.pipeline.coverage.IntervalMerger;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

public class AdherenceProcessFunction extends
        KeyedBroadcastProcessFunction<Tuple2<String, String>, EnrichedFillEvent, AlertLeadTimeUpdate, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(AdherenceProcessFunction.class);

    // Same reasoning as ChronicClassFilterFunction: a count, logged sparsely, so an
    // empty broadcast is visible. Per subtask, reset on restart — a log, not a metric.
    private static final long BROADCAST_LOG_EVERY = 500L;
    private transient long leadTimeEntriesApplied;

    public static final OutputTag<GapRiskAlert> GAP_RISK_ALERT_TAG =
            new OutputTag<>("gap-risk-alert") {
            };

    public static final OutputTag<LapsedAlert> LAPSED_ALERT_TAG =
            new OutputTag<>("lapsed-alert") {
            };

    public static final MapStateDescriptor<String, Integer> LEAD_TIME_DESCRIPTOR =
            new MapStateDescriptor<>("lead-time-state", Types.STRING, Types.INT);

    public static final OutputTag<PdcSnapshot> PDC_SNAPSHOT_OUTPUT_TAG =
            new OutputTag<>("pdc-snapshot") {
            };

    private final StateBackEndConfig stateBackEndConfig;
    private final int defaultAlertLeadDays;

    private transient ValueState<AdherenceState> adherenceState;
    private transient Counter missingLeadTimeCounter;
    private transient Counter gapRiskAlertsEmittedCounter;
    private transient Counter lapsedAlertsEmittedCounter;

    public AdherenceProcessFunction(StateBackEndConfig stateBackEndConfig,
                                    int defaultAlertLeadDays) {
        this.stateBackEndConfig = stateBackEndConfig;
        this.defaultAlertLeadDays = defaultAlertLeadDays;
    }

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<AdherenceState> descriptor =
                new ValueStateDescriptor<>("adherence-state", TypeInformation.of(AdherenceState.class));
        descriptor.enableTimeToLive(stateBackEndConfig.toStateTtlConfig());
        adherenceState = getRuntimeContext().getState(descriptor);
        AdherenceMetricsReporter metrics = AdherenceMetricsReporter.register(getRuntimeContext());
        missingLeadTimeCounter = metrics.missingLeadTimeLookup();
        gapRiskAlertsEmittedCounter = metrics.gapRiskAlertsEmitted();
        lapsedAlertsEmittedCounter = metrics.lapsedAlertsEmitted();

        leadTimeEntriesApplied = 0L;
    }

    @Override
    public void processElement(EnrichedFillEvent enrichedFillEvent,
                               ReadOnlyContext context
            , Collector<Void> collector) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing fill event: claimId={} drugClass={} eventType={} fillDate={}",
                    enrichedFillEvent.event().claimId(), enrichedFillEvent.drugClass(),
                    enrichedFillEvent.event().eventType(), enrichedFillEvent.event().fillDate());
        }
        if (enrichedFillEvent.event().eventType() == EventType.REVERSAL) {
            handleReversal(enrichedFillEvent, context);
            return;
        }
        AdherenceState currentState = adherenceState.value();
        if (currentState == null) {
            currentState = new AdherenceState(null,
                    null, 0, List.of(), 0, null, null);
        }

        IntervalMerger.MergeResult mergeResult = IntervalMerger.merge(currentState, enrichedFillEvent.event());
        AdherenceState merged = mergeResult.state();
        if (mergeResult.outcome() == IntervalMerger.MergeOutcome.DUPLICATE_CLAIM) {
            LOG.warn("Duplicate claimId already merged, ignoring: claimId={} drugClass={}",
                    enrichedFillEvent.event().claimId(), enrichedFillEvent.drugClass());
            return;
        }

        if (currentState.activeTimerTimestamp() != null) {
            context.timerService().deleteEventTimeTimer(currentState.activeTimerTimestamp());
        }

        String compositeKey = enrichedFillEvent.drugClass() + "|" + enrichedFillEvent.event().dispensingChanel().name();
        ReadOnlyBroadcastState<String, Integer> leadTimeState = context.getBroadcastState(LEAD_TIME_DESCRIPTOR);
        Integer alertLeadDays = leadTimeState.get(compositeKey);
        if (alertLeadDays == null) {
            missingLeadTimeCounter.inc();
            LOG.debug("No alert lead time for key={}, falling back to the configured default of {} days",
                    compositeKey, defaultAlertLeadDays);
            alertLeadDays = defaultAlertLeadDays;
        }

        long timerTimestamp = merged.currentSupplyEndDate()
                .minusDays(alertLeadDays)
                .atStartOfDay(ZoneOffset.UTC)
                .toInstant()
                .toEpochMilli();
        context.timerService().registerEventTimeTimer(timerTimestamp);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Registered GAP_RISK timer: claimId={} drugClass={} alertLeadDays={} "
                            + "timerTs={} timerAt={} replacedTimerTs={}",
                    enrichedFillEvent.event().claimId(), enrichedFillEvent.drugClass(), alertLeadDays,
                    timerTimestamp, Instant.ofEpochMilli(timerTimestamp), currentState.activeTimerTimestamp());
        }


        AdherenceState finalState = new AdherenceState(
                merged.currentSupplyEndDate(), merged.lastFillDate(), merged.totalDaysCovered(),
                merged.activeCoverageIntervals(), alertLeadDays, timerTimestamp, TimerStage.GAP_RISK
        );
        adherenceState.update(finalState);

        context.output(PDC_SNAPSHOT_OUTPUT_TAG, new PdcSnapshot(
                enrichedFillEvent.event().memberId(), enrichedFillEvent.drugClass(), finalState.totalDaysCovered(),
                finalState.currentSupplyEndDate(), context.timestamp()));
    }

    private void handleReversal(EnrichedFillEvent event, ReadOnlyContext context) throws IOException {
        AdherenceState currentState = adherenceState.value();
        if (currentState == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Reversal for a key with no state, ignoring: claimId={} originalClaimId={}",
                        event.event().claimId(), event.event().originalClaimId());
            }
            return;
        }
        RxFillEvent reversal = event.event();
        IntervalMerger.UnwindResult unwoundResult = IntervalMerger.unwind(currentState, reversal.originalClaimId());
        AdherenceState unwound = unwoundResult.state();

        if (unwoundResult.outcome() == IntervalMerger.UnwindOutcome.NO_MATCHING_INTERVAL) {
            LOG.warn("Reversal matched no interval, ignoring: claimId={} originalClaimId={} drugClass={}",
                    reversal.claimId(), reversal.originalClaimId(), event.drugClass());
            return;
        }

        if (currentState.activeTimerTimestamp() != null) {
            context.timerService().deleteEventTimeTimer(currentState.activeTimerTimestamp());
        }

        String memberId = reversal.memberId();
        String drugClass = event.drugClass();

        if (unwound.currentSupplyEndDate() == null) {
            // D34: no coverage remains — binding correction guarantee, immediate LapsedAlert, no timer
            context.output(LAPSED_ALERT_TAG, new LapsedAlert(
                    UUID.randomUUID().toString(), memberId, drugClass,
                    reversal.fillDate(), context.timestamp()));
            lapsedAlertsEmittedCounter.inc();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Reversal left zero coverage, corrective LapsedAlert emitted immediately: "
                                + "claimId={} originalClaimId={} memberId={} drugClass={}",
                        reversal.claimId(), reversal.originalClaimId(), memberId, drugClass);
            }

            adherenceState.update(new AdherenceState(
                    null, unwound.lastFillDate(), 0, unwound.activeCoverageIntervals(),
                    unwound.alertLeadDays(), null, null));
            return;
        }

        // Coverage remains: re-arm the early-warning timer against the recomputed end date —
        // this is what makes its eventual firing "supersede" whatever alert was already emitted.
        long newTimerTimestamp = unwound.currentSupplyEndDate()
                .minusDays(unwound.alertLeadDays())
                .atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        context.timerService().registerEventTimeTimer(newTimerTimestamp);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Reversal left coverage, GAP_RISK timer re-armed: claimId={} originalClaimId={} "
                            + "timerTs={} timerAt={}",
                    reversal.claimId(), reversal.originalClaimId(),
                    newTimerTimestamp, Instant.ofEpochMilli(newTimerTimestamp));
        }

        adherenceState.update(new AdherenceState(
                unwound.currentSupplyEndDate(), unwound.lastFillDate(), unwound.totalDaysCovered(),
                unwound.activeCoverageIntervals(), unwound.alertLeadDays(),
                newTimerTimestamp, TimerStage.GAP_RISK));
    }

    @Override
    public void processBroadcastElement(AlertLeadTimeUpdate alertLeadTimeUpdate,
                                        Context ctx,
                                        Collector<Void> collector) throws Exception {
        ctx.getBroadcastState(LEAD_TIME_DESCRIPTOR).put(alertLeadTimeUpdate.drugClassAndChannel(), alertLeadTimeUpdate.alertLeadDays());

        leadTimeEntriesApplied++;
        if (leadTimeEntriesApplied == 1L || leadTimeEntriesApplied % BROADCAST_LOG_EVERY == 0L) {
            LOG.info("Alert lead time broadcast applied {} entries on this subtask", leadTimeEntriesApplied);
        }
    }

    AdherenceState currentadherenceState() throws IOException {
        return adherenceState.value();
    }

    public long missingLeadTimeCount() {
        return missingLeadTimeCounter.getCount();
    }

    public long gapRiskAlertsEmittedCount() {
        return gapRiskAlertsEmittedCounter.getCount();
    }

    public long lapsedAlertsEmittedCount() {
        return lapsedAlertsEmittedCounter.getCount();
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Void> out) throws Exception {
        AdherenceState state = adherenceState.value();
        if (isStaleTimer(state, timestamp)) {
            logStaleTimer(state, ctx, timestamp);
            return; // stale/orphaned timer — no longer matches what's currently active
        }

        String memberId = ctx.getCurrentKey().f0;
        String drugClass = ctx.getCurrentKey().f1;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Timer fired: stage={} memberId={} drugClass={} firedTs={} firedAt={}",
                    state.activeTimerStage(), memberId, drugClass,
                    timestamp, Instant.ofEpochMilli(timestamp));
        }

        if (state.activeTimerStage() == TimerStage.GAP_RISK) {
            handleGapRiskTimer(state, ctx, timestamp, memberId, drugClass);
        } else if (state.activeTimerStage() == TimerStage.LAPSED) {
            handleLapsedTimer(state, ctx, timestamp, memberId, drugClass);
        }
    }

    private void handleLapsedTimer(AdherenceState state, OnTimerContext ctx, long timestamp,
                                   String memberId, String drugClass) throws IOException {
        String alertId = UUID.randomUUID().toString();
        ctx.output(LAPSED_ALERT_TAG, new LapsedAlert(
                alertId, memberId, drugClass,
                state.currentSupplyEndDate(), timestamp));
        lapsedAlertsEmittedCounter.inc();

        if (LOG.isDebugEnabled()) {
            LOG.debug("LapsedAlert emitted, no timer pending until the next fill: alertId={} memberId={} "
                            + "drugClass={} supplyEndDate={} firedTs={}",
                    alertId, memberId, drugClass, state.currentSupplyEndDate(), timestamp);
        }

        adherenceState.update(new AdherenceState(
                state.currentSupplyEndDate(), state.lastFillDate(), state.totalDaysCovered(),
                state.activeCoverageIntervals(), state.alertLeadDays(),
                null, null)); // no more timer pending until the next fill restarts the cycle
    }

    private void handleGapRiskTimer(AdherenceState state, OnTimerContext ctx, long timestamp,
                                    String memberId, String drugClass) throws IOException {
        long projectedExhaustionMillis = state.currentSupplyEndDate()
                .atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        if (projectedExhaustionMillis <= timestamp) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Supply already exhausted at firing, no gap-risk alert: memberId={} "
                                + "drugClass={} supplyEndDate={} firedTs={}",
                        memberId, drugClass, state.currentSupplyEndDate(), timestamp);
            }
            return; // supply already exhausted relative to this firing — defensive no-op, spec step 2
        }

        String alertId = UUID.randomUUID().toString();
        ctx.output(GAP_RISK_ALERT_TAG, new GapRiskAlert(
                alertId, memberId, drugClass,
                state.currentSupplyEndDate(), state.alertLeadDays(), timestamp));
        gapRiskAlertsEmittedCounter.inc();

        ctx.timerService().registerEventTimeTimer(projectedExhaustionMillis);

        if (LOG.isDebugEnabled()) {
            LOG.debug("GapRiskAlert emitted, LAPSED timer registered: alertId={} memberId={} drugClass={} "
                            + "supplyEndDate={} alertLeadDays={} nextTimerTs={} nextTimerAt={}",
                    alertId, memberId, drugClass, state.currentSupplyEndDate(), state.alertLeadDays(),
                    projectedExhaustionMillis, Instant.ofEpochMilli(projectedExhaustionMillis));
        }

        adherenceState.update(new AdherenceState(
                state.currentSupplyEndDate(), state.lastFillDate(), state.totalDaysCovered(),
                state.activeCoverageIntervals(), state.alertLeadDays(),
                projectedExhaustionMillis, TimerStage.LAPSED));
    }

    private static boolean isStaleTimer(AdherenceState state, long timestamp) {
        return state == null || state.activeTimerTimestamp() == null
                || !state.activeTimerTimestamp().equals(timestamp);
    }

    private void logStaleTimer(AdherenceState state, OnTimerContext ctx, long timestamp) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Ignoring stale timer: key={} firedTs={} firedAt={} activeTimerTs={} reason={}",
                    ctx.getCurrentKey(), timestamp, Instant.ofEpochMilli(timestamp),
                    state == null ? null : state.activeTimerTimestamp(), staleTimerReason(state));
        }
    }

    /**
     * Three different situations produce the same silent return, and only one of them is
     * suspicious: "timer-superseded" is the normal refill case, "no-active-timer" and
     * "no-state" mean a timer outlived the state it belonged to. #148.
     */
    private static String staleTimerReason(AdherenceState state) {
        if (state == null) {
            return "no-state";
        }
        if (state.activeTimerTimestamp() == null) {
            return "no-active-timer";
        }
        return "timer-superseded";
    }

    void forceAdherenceStateForTest(AdherenceState state) throws IOException {
        adherenceState.update(state);
    }
}
