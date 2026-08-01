package com.healthcare.rxvigilance.pipeline.operators;

import com.healthcare.rxvigilance.config.StateBackEndConfig;
import com.healthcare.rxvigilance.domain.*;
import com.healthcare.rxvigilance.domain.enums.Channel;
import com.healthcare.rxvigilance.domain.enums.EventType;
import com.healthcare.rxvigilance.serialization.kryo.RecordKryoSerializer;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AdherenceProcessFunctionTest {
    private KeyedBroadcastOperatorTestHarness<Tuple2<String, String>, EnrichedFillEvent, AlertLeadTimeUpdate, Void> harness;
    private static final int DEFAULT_LEAD_DAYS = 7;


    @BeforeEach
    void setUp() throws Exception {
        AdherenceProcessFunction function =
                new AdherenceProcessFunction(new StateBackEndConfig(400), DEFAULT_LEAD_DAYS);

        CoBroadcastWithKeyedOperator<Tuple2<String, String>, EnrichedFillEvent, AlertLeadTimeUpdate, Void> operator =
                new CoBroadcastWithKeyedOperator<>(function, List.of(AdherenceProcessFunction.LEAD_TIME_DESCRIPTOR));

        KeySelector<EnrichedFillEvent, Tuple2<String, String>> keySelector = enrichedFillEvent ->
                Tuple2.of(enrichedFillEvent.event().memberId(), enrichedFillEvent.drugClass());

        harness = new KeyedBroadcastOperatorTestHarness<>(
                operator, keySelector,
                TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
                }),
                1, 1, 0);

        harness.getExecutionConfig().registerTypeWithKryoSerializer(EnrichedFillEvent.class, RecordKryoSerializer.class);
        harness.getExecutionConfig().registerTypeWithKryoSerializer(RxFillEvent.class, RecordKryoSerializer.class);
        harness.getExecutionConfig().registerTypeWithKryoSerializer(AdherenceState.class, RecordKryoSerializer.class);
        harness.getExecutionConfig().registerTypeWithKryoSerializer(CoverageInterval.class, RecordKryoSerializer.class);
        harness.getExecutionConfig().registerTypeWithKryoSerializer(PdcSnapshot.class, RecordKryoSerializer.class);

        harness.open();
        harness.processBroadcastWatermark(Long.MAX_VALUE);
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    @Test
    void singleFillRegistersTimerAtEndDateMinusLeadDays() throws Exception {
        harness.processBroadcastElement(new AlertLeadTimeUpdate("CHRONIC_CARDIAC|RETAIL", 5), 0L);

        LocalDate fillDate = LocalDate.of(2026, Month.MARCH, 1);
        int daySupply = 30;
        EnrichedFillEvent event = fillEvent("claim-1", "member-1", "CHRONIC_CARDIAC", fillDate, daySupply, Channel.RETAIL);
        harness.processElement(event, epochMillis(fillDate));

        assertThat(harness.numEventTimeTimers()).isEqualTo(1);
        LocalDate expectedEndDate = fillDate.plusDays(daySupply);
        long expectedTimerTimestamp = epochMillis(expectedEndDate.minusDays(5));

        AdherenceState state = function().currentadherenceState();
        assertThat(state.currentSupplyEndDate()).isEqualTo(expectedEndDate);
        assertThat(state.alertLeadDays()).isEqualTo(5);
        assertThat(state.activeTimerTimestamp()).isEqualTo(expectedTimerTimestamp);

        harness.processWatermark(expectedTimerTimestamp);
        assertThat(harness.numEventTimeTimers()).isZero();
    }

    @Test
    void refillBeforeThresholdCancelsAndRegistersExactlyOnceTimer() throws Exception {
        harness.processBroadcastElement(new AlertLeadTimeUpdate("CHRONIC_CARDIAC|RETAIL", 5), 0L);

        LocalDate firstFillDate = LocalDate.of(2026, Month.MARCH, 1);
        harness.processElement(
                fillEvent("claim-1", "member-1", "CHRONIC_CARDIAC", firstFillDate, 30, Channel.RETAIL),
                epochMillis(firstFillDate));
        assertThat(harness.numEventTimeTimers()).isEqualTo(1);
        long firstTimerTimestamp = function().currentadherenceState().activeTimerTimestamp();

        LocalDate secondFillDate = firstFillDate.plusDays(10);
        harness.processElement(
                fillEvent("claim-2", "member-1", "CHRONIC_CARDIAC", secondFillDate, 30, Channel.RETAIL),
                epochMillis(secondFillDate)
        );

        assertThat(harness.numEventTimeTimers()).isEqualTo(1);
        long secondTimerTimestamp = function().currentadherenceState().activeTimerTimestamp();
        assertThat(secondTimerTimestamp).isNotEqualTo(firstTimerTimestamp);
    }

    @Test
    void leadTimeResolvedPerClassAndChannelNotAConstant() throws Exception {
        harness.processBroadcastElement(new AlertLeadTimeUpdate("CHRONIC_CARDIAC|RETAIL", 3), 0L);
        harness.processBroadcastElement(new AlertLeadTimeUpdate("CHRONIC_CARDIAC|MAIL_ORDER", 10), 0L);

        LocalDate fillDate = LocalDate.of(2026, Month.MARCH, 1);

        harness.processElement(
                fillEvent("claim-1", "member-retail", "CHRONIC_CARDIAC", fillDate, 30, Channel.RETAIL),
                epochMillis(fillDate));
        int retailLeadDays = function().currentadherenceState().alertLeadDays();

        harness.processElement(
                fillEvent("claim-2", "member-mail", "CHRONIC_CARDIAC", fillDate, 30, Channel.MAIL_ORDER),
                epochMillis(fillDate));
        int mailOrderLeadDays = function().currentadherenceState().alertLeadDays();

        assertThat(retailLeadDays).isEqualTo(3);
        assertThat(mailOrderLeadDays).isEqualTo(10);
    }

    @Test
    void pdcSnapshotEmittedOnFill() throws Exception {
        harness.processBroadcastElement(new AlertLeadTimeUpdate("CHRONIC_CARDIAC|RETAIL", 5), 0L);
        LocalDate fillDate = LocalDate.of(2026, Month.MARCH, 1);
        harness.processElement(
                fillEvent("claim-1", "member-1", "CHRONIC_CARDIAC", fillDate, 30, Channel.RETAIL),
                epochMillis(fillDate));

        List<PdcSnapshot> snapshots = harness.getSideOutput(AdherenceProcessFunction.PDC_SNAPSHOT_OUTPUT_TAG)
                .stream().map(StreamRecord::getValue).toList();

        assertThat(snapshots).containsExactly(new PdcSnapshot(
                "member-1", "CHRONIC_CARDIAC", 30, fillDate.plusDays(30), epochMillis(fillDate)));

    }

    @Test
    void missingLeadTimeLookupFallsBackToDefaultAndWarns() throws Exception {
        // no broadcast update at all for "CHRONIC_CARDIAC|RETAIL"
        LocalDate fillDate = LocalDate.of(2026, Month.MARCH, 1);
        harness.processElement(
                fillEvent("claim-1", "member-1", "CHRONIC_CARDIAC", fillDate, 30, Channel.RETAIL),
                epochMillis(fillDate));

        assertThat(function().currentadherenceState().alertLeadDays()).isEqualTo(DEFAULT_LEAD_DAYS);
    }

    private EnrichedFillEvent fillEvent(String claimId, String memberId, String drugClass,
                                        LocalDate fillDate, int daySupply, Channel channel) {
        RxFillEvent event = new RxFillEvent(
                EventType.FILL, claimId, memberId, "NDC-1",
                fillDate, daySupply, new BigDecimal("30.00"),
                "pharmacy-1", "rx-1", 2, channel, null);
        return new EnrichedFillEvent(event, drugClass);
    }

    private long epochMillis(LocalDate date) {
        return date.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    }

    private AdherenceProcessFunction function() {
        return (AdherenceProcessFunction) ((CoBroadcastWithKeyedOperator<Tuple2<String, String>, EnrichedFillEvent, AlertLeadTimeUpdate, Void>)
                harness.getOperator()).getUserFunction();
    }
}
