package com.healthcare.rxvigilance.pipeline.operators;

import com.healthcare.rxvigilance.config.StateBackEndConfig;
import com.healthcare.rxvigilance.domain.AdherenceState;
import com.healthcare.rxvigilance.domain.AlertLeadTimeUpdate;
import com.healthcare.rxvigilance.domain.EnrichedFillEvent;
import com.healthcare.rxvigilance.domain.PdcSnapshot;
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

public class AdherenceProcessFunction extends
        KeyedBroadcastProcessFunction<Tuple2<String, String>, EnrichedFillEvent, AlertLeadTimeUpdate, Void> {

    public static final MapStateDescriptor<String, Integer> LEAD_TIME_DESCRIPTOR =
            new MapStateDescriptor<>("lead-time-state", Types.STRING, Types.INT);

    public static final OutputTag<PdcSnapshot> PDC_SNAPSHOT_OUTPUT_TAG =
            new OutputTag<>("pdc-snapshot") {};

    private final StateBackEndConfig stateBackEndConfig;
    private final int defaultAlertLeadDays;

    private transient ValueState<AdherenceState> adherenceState;
    private transient Counter missingLeadTimeCounter;

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
        missingLeadTimeCounter = getRuntimeContext().getMetricGroup().counter("missingLeadTimeLookup");
    }

    @Override
    public void processElement(EnrichedFillEvent enrichedFillEvent,
            ReadOnlyContext context
            , Collector<Void> collector) throws Exception {
        AdherenceState currentState = adherenceState.value();
        if(currentState == null){
            currentState = new AdherenceState(null,null,0, List.of(),0,null);
        }

        AdherenceState merged = IntervalMerger.merge(currentState,enrichedFillEvent.event());
        if(merged == currentState) {
            return;
        }

        if(currentState.activeTimerTimestamp() != null) {
            context.timerService().deleteEventTimeTimer(currentState.activeTimerTimestamp());
        }

        String compositeKey = enrichedFillEvent.drugClass() + "|" + enrichedFillEvent.event().dispensingChanel().name();
        ReadOnlyBroadcastState<String,Integer> leadTimeState = context.getBroadcastState(LEAD_TIME_DESCRIPTOR);
        Integer alertLeadDays = leadTimeState.get(compositeKey);
        if(alertLeadDays == null){
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
                merged.currentSupplyEndDate(),merged.lastFillDate(),merged.totalDaysCovered(),
                merged.activeCoverageIntervals(),alertLeadDays,timerTimestamp
        );
        adherenceState.update(finalState);

        context.output(PDC_SNAPSHOT_OUTPUT_TAG, new PdcSnapshot(
                enrichedFillEvent.event().memberId(), enrichedFillEvent.drugClass(), finalState.totalDaysCovered(),
                finalState.currentSupplyEndDate(), context.timestamp()));
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
}
