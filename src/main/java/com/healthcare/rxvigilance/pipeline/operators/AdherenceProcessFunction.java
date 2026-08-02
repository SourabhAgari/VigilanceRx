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

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

public class AdherenceProcessFunction extends
        KeyedBroadcastProcessFunction<Tuple2<String, String>, EnrichedFillEvent, AlertLeadTimeUpdate, Void> {

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
    }

    @Override
    public void processElement(EnrichedFillEvent enrichedFillEvent,
                               ReadOnlyContext context
            , Collector<Void> collector) throws Exception {
        if (enrichedFillEvent.event().eventType() == EventType.REVERSAL) {
            handleReversal(enrichedFillEvent, context);
            return;
        }
        AdherenceState currentState = adherenceState.value();
        if (currentState == null) {
            currentState = new AdherenceState(null,
                    null, 0, List.of(), 0, null, null);
        }

        AdherenceState merged = IntervalMerger.merge(currentState, enrichedFillEvent.event());
        if (merged == currentState) {
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
            alertLeadDays = defaultAlertLeadDays;
        }

        long timerTimestamp = merged.currentSupplyEndDate()
                .minusDays(alertLeadDays)
                .atStartOfDay(ZoneOffset.UTC)
                .toInstant()
                .toEpochMilli();
        context.timerService().registerEventTimeTimer(timerTimestamp);

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
            return;
        }
        RxFillEvent reversal = event.event();
        AdherenceState unwound = IntervalMerger.unwind(currentState, reversal.originalClaimId());

        if (unwound == currentState) {
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
    }

    AdherenceState currentadherenceState() throws IOException {
        return adherenceState.value();
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Void> out) throws Exception {
        AdherenceState state = adherenceState.value();
        if (state == null || state.activeTimerTimestamp() == null
                || !state.activeTimerTimestamp().equals(timestamp)) {
            return; // stale/orphaned timer — no longer matches what's currently active
        }

        String memberId = ctx.getCurrentKey().f0;
        String drugClass = ctx.getCurrentKey().f1;

        if (state.activeTimerStage() == TimerStage.GAP_RISK) {
            long projectedExhaustionMillis = state.currentSupplyEndDate()
                    .atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
            if (projectedExhaustionMillis <= timestamp) {
                return; // supply already exhausted relative to this firing — defensive no-op, spec step 2
            }

            ctx.output(GAP_RISK_ALERT_TAG, new GapRiskAlert(
                    UUID.randomUUID().toString(), memberId, drugClass,
                    state.currentSupplyEndDate(), state.alertLeadDays(), timestamp));
            gapRiskAlertsEmittedCounter.inc();

            ctx.timerService().registerEventTimeTimer(projectedExhaustionMillis);

            adherenceState.update(new AdherenceState(
                    state.currentSupplyEndDate(), state.lastFillDate(), state.totalDaysCovered(),
                    state.activeCoverageIntervals(), state.alertLeadDays(),
                    projectedExhaustionMillis, TimerStage.LAPSED));

        } else if (state.activeTimerStage() == TimerStage.LAPSED) {
            ctx.output(LAPSED_ALERT_TAG, new LapsedAlert(
                    UUID.randomUUID().toString(), memberId, drugClass,
                    state.currentSupplyEndDate(), timestamp));
            lapsedAlertsEmittedCounter.inc();

            adherenceState.update(new AdherenceState(
                    state.currentSupplyEndDate(), state.lastFillDate(), state.totalDaysCovered(),
                    state.activeCoverageIntervals(), state.alertLeadDays(),
                    null, null)); // no more timer pending until the next fill restarts the cycle
        }
    }

    void forceAdherenceStateForTest(AdherenceState state) throws IOException {
        adherenceState.update(state);
    }
}
