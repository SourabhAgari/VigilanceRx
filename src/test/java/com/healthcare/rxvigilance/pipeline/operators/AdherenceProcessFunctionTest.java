package com.healthcare.rxvigilance.pipeline.operators;

import com.healthcare.rxvigilance.config.StateBackEndConfig;
import com.healthcare.rxvigilance.domain.*;
import com.healthcare.rxvigilance.domain.enums.Channel;
import com.healthcare.rxvigilance.domain.enums.EventType;
import com.healthcare.rxvigilance.domain.enums.TimerStage;
import com.healthcare.rxvigilance.logging.LogCapture;
import com.healthcare.rxvigilance.metrics.AdherenceMetricsReporter;
import com.healthcare.rxvigilance.serialization.kryo.RecordKryoSerializer;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.apache.logging.log4j.Level;
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
        harness.getExecutionConfig().registerTypeWithKryoSerializer(GapRiskAlert.class, RecordKryoSerializer.class);
        harness.getExecutionConfig().registerTypeWithKryoSerializer(LapsedAlert.class, RecordKryoSerializer.class);

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

        assertThat(function().metrics().count(AdherenceMetricsReporter.TIMERS_REGISTERED)).isEqualTo(1L);
        assertThat(harness.numEventTimeTimers()).isEqualTo(1);
        LocalDate expectedEndDate = fillDate.plusDays(daySupply);
        long expectedTimerTimestamp = epochMillis(expectedEndDate.minusDays(5));

        AdherenceState state = function().currentadherenceState();
        assertThat(state.currentSupplyEndDate()).isEqualTo(expectedEndDate);
        assertThat(state.alertLeadDays()).isEqualTo(5);
        assertThat(state.activeTimerTimestamp()).isEqualTo(expectedTimerTimestamp);

        harness.processWatermark(expectedTimerTimestamp);

        assertThat(harness.numEventTimeTimers()).isEqualTo(1);

        List<GapRiskAlert> alerts = harness.getSideOutput(AdherenceProcessFunction.GAP_RISK_ALERT_TAG)
                .stream().map(StreamRecord::getValue).toList();
        assertThat(alerts).containsExactly(new GapRiskAlert(
                alerts.get(0).alertId(), "member-1", "CHRONIC_CARDIAC",
                expectedEndDate, 5, expectedTimerTimestamp));

        AdherenceState afterFiring = function().currentadherenceState();
        assertThat(afterFiring.activeTimerStage()).isEqualTo(TimerStage.LAPSED);
        assertThat(afterFiring.activeTimerTimestamp()).isEqualTo(epochMillis(expectedEndDate));
        assertThat(function().metrics().count(AdherenceMetricsReporter.TIMERS_FIRED)).isEqualTo(1L);
        assertThat(function().metrics().count(AdherenceMetricsReporter.TIMERS_REGISTERED)).isEqualTo(2L);
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

        // numEventTimeTimers is 1 because the first was deleted; the counter is 2 because it
        // counts registrations over time, not live timers. Different questions, different answers.
        assertThat(function().metrics().count(AdherenceMetricsReporter.TIMERS_REGISTERED)).isEqualTo(2L);
        assertThat(function().metrics().count(AdherenceMetricsReporter.TIMERS_FIRED)).isZero();

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
        assertThat(function().missingLeadTimeCount()).isZero();
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
        assertThat(function().metrics().count(AdherenceMetricsReporter.PDC_SNAPSHOTS_EMITTED)).isEqualTo(1L);

    }

    @Test
    void missingLeadTimeLookupFallsBackToDefaultAndWarns() throws Exception {
        // no broadcast update at all for "CHRONIC_CARDIAC|RETAIL"
        LocalDate fillDate = LocalDate.of(2026, Month.MARCH, 1);
        harness.processElement(
                fillEvent("claim-1", "member-1", "CHRONIC_CARDIAC", fillDate, 30, Channel.RETAIL),
                epochMillis(fillDate));

        assertThat(function().currentadherenceState().alertLeadDays()).isEqualTo(DEFAULT_LEAD_DAYS);
        assertThat(function().missingLeadTimeCount()).isEqualTo(1);
    }

    private EnrichedFillEvent fillEvent(String claimId, String memberId, String drugClass,
                                        LocalDate fillDate, int daySupply, Channel channel) {
        RxFillEvent event = new RxFillEvent(
                EventType.FILL, claimId, memberId, "NDC-1",
                fillDate, daySupply, new BigDecimal("30.00"),
                "pharmacy-1", "rx-1", 2, channel, null);
        return new EnrichedFillEvent(event, drugClass);
    }

    @Test
    void noRefillEmitsGapRiskAlertThenLapsedAlertInEventTimeOrder() throws Exception {
        harness.processBroadcastElement(new AlertLeadTimeUpdate("CHRONIC_CARDIAC|RETAIL", 5), 0L);

        LocalDate fillDate = LocalDate.of(2026, Month.MARCH, 1);
        int daySupply = 30;
        harness.processElement(
                fillEvent("claim-1", "member-1", "CHRONIC_CARDIAC", fillDate, daySupply, Channel.RETAIL),
                epochMillis(fillDate));

        LocalDate expectedEndDate = fillDate.plusDays(daySupply);
        long gapRiskTimerTimestamp = epochMillis(expectedEndDate.minusDays(5));
        long lapsedTimerTimestamp = epochMillis(expectedEndDate);

        // no refill ever arrives — advance the watermark straight through both deadlines
        harness.processWatermark(gapRiskTimerTimestamp);
        harness.processWatermark(lapsedTimerTimestamp);

        List<GapRiskAlert> gapRiskAlerts = harness.getSideOutput(AdherenceProcessFunction.GAP_RISK_ALERT_TAG)
                .stream().map(StreamRecord::getValue).toList();
        List<LapsedAlert> lapsedAlerts = harness.getSideOutput(AdherenceProcessFunction.LAPSED_ALERT_TAG)
                .stream().map(StreamRecord::getValue).toList();

        assertThat(function().metrics().count(AdherenceMetricsReporter.TIMERS_FIRED)).isEqualTo(2L);
        assertThat(function().metrics().count(AdherenceMetricsReporter.TIMERS_REGISTERED)).isEqualTo(2L);

        assertThat(gapRiskAlerts).hasSize(1);
        assertThat(gapRiskAlerts.get(0).memberId()).isEqualTo("member-1");
        assertThat(gapRiskAlerts.get(0).drugClass()).isEqualTo("CHRONIC_CARDIAC");
        assertThat(gapRiskAlerts.get(0).expiresOn()).isEqualTo(expectedEndDate);
        assertThat(gapRiskAlerts.get(0).leadDays()).isEqualTo(5);
        assertThat(gapRiskAlerts.get(0).emittedAt()).isEqualTo(gapRiskTimerTimestamp);

        assertThat(lapsedAlerts).hasSize(1);
        assertThat(lapsedAlerts.get(0).memberId()).isEqualTo("member-1");
        assertThat(lapsedAlerts.get(0).drugClass()).isEqualTo("CHRONIC_CARDIAC");
        assertThat(lapsedAlerts.get(0).lapsedOn()).isEqualTo(expectedEndDate);
        assertThat(lapsedAlerts.get(0).emittedAt()).isEqualTo(lapsedTimerTimestamp);

        // "in event-time order" — GapRiskAlert's own timestamp must precede LapsedAlert's
        assertThat(gapRiskAlerts.get(0).emittedAt()).isLessThan(lapsedAlerts.get(0).emittedAt());

        // lapsed fired and nothing re-registered — the cycle terminates correctly
        assertThat(harness.numEventTimeTimers()).isZero();
        AdherenceState finalState = function().currentadherenceState();
        assertThat(finalState.activeTimerTimestamp()).isNull();
        assertThat(finalState.activeTimerStage()).isNull();

        assertThat(function().gapRiskAlertsEmittedCount()).isEqualTo(1);
        assertThat(function().lapsedAlertsEmittedCount()).isEqualTo(1);
    }

    @Test
    void staleTimerFiresAsNoOp() throws Exception {
        harness.processBroadcastElement(new AlertLeadTimeUpdate("CHRONIC_CARDIAC|RETAIL", 5), 0L);

        LocalDate fillDate = LocalDate.of(2026, Month.MARCH, 1);
        int daySupply = 30;
        harness.processElement(
                fillEvent("claim-1", "member-1", "CHRONIC_CARDIAC", fillDate, daySupply, Channel.RETAIL),
                epochMillis(fillDate));

        LocalDate expectedEndDate = fillDate.plusDays(daySupply);
        long realTimerTimestamp = epochMillis(expectedEndDate.minusDays(5));

        // Simulate a stale-timer scenario: state's activeTimerTimestamp no longer matches
        // the timer actually registered with Flink (e.g., restored from a savepoint older
        // than the live timer schedule). The real, still-registered timer stays at
        // realTimerTimestamp; we overwrite state to claim a *different* one is active.
        AdherenceState desynced = function().currentadherenceState();
        function().forceAdherenceStateForTest(new AdherenceState(
                desynced.currentSupplyEndDate(), desynced.lastFillDate(), desynced.totalDaysCovered(),
                desynced.activeCoverageIntervals(), desynced.alertLeadDays(),
                realTimerTimestamp + 1, TimerStage.GAP_RISK));

        harness.processWatermark(realTimerTimestamp);

        assertThat(harness.getSideOutput(AdherenceProcessFunction.GAP_RISK_ALERT_TAG)).isNullOrEmpty();
        assertThat(harness.numEventTimeTimers()).isZero(); // fired (Flink removes it regardless) but no-op, nothing cascaded
        assertThat(function().gapRiskAlertsEmittedCount()).isZero();

        // Counted even though it did nothing. If this only counted useful firings, "every timer
        // is stale" and "the watermark is frozen" would both read as timersFired == 0.
        assertThat(function().metrics().count(AdherenceMetricsReporter.TIMERS_FIRED)).isEqualTo(1L);
        assertThat(function().metrics().count(AdherenceMetricsReporter.TIMERS_REGISTERED)).isEqualTo(1L);
    }

    @Test
    void reversalShrinkingCoverageRegistersSupersedingTimerFromRecomputedEndDate() throws Exception {
        harness.processBroadcastElement(new AlertLeadTimeUpdate("CHRONIC_CARDIAC|RETAIL", 5), 0L);

        LocalDate firstFillDate = LocalDate.of(2026, Month.MARCH, 1);
        harness.processElement(
                fillEvent("claim-1", "member-1", "CHRONIC_CARDIAC", firstFillDate, 30, Channel.RETAIL),
                epochMillis(firstFillDate));

        LocalDate secondFillDate = LocalDate.of(2026, Month.MARCH, 15);
        harness.processElement(
                fillEvent("claim-2", "member-1", "CHRONIC_CARDIAC", secondFillDate, 30, Channel.RETAIL),
                epochMillis(secondFillDate));

        LocalDate extendedEndDate = secondFillDate.plusDays(30);
        long extendedTimerTimestamp = function().currentadherenceState().activeTimerTimestamp();
        assertThat(function().currentadherenceState().currentSupplyEndDate()).isEqualTo(extendedEndDate);

        // reverse the second fill — coverage shrinks back to just the first fill's interval
        LocalDate reversalDate = secondFillDate.plusDays(1);
        harness.processElement(
                reversalEvent("claim-3", "member-1", "CHRONIC_CARDIAC", reversalDate, "claim-2"),
                epochMillis(reversalDate));

        LocalDate shrunkEndDate = firstFillDate.plusDays(30);
        AdherenceState afterReversal = function().currentadherenceState();
        assertThat(afterReversal.currentSupplyEndDate()).isEqualTo(shrunkEndDate);
        assertThat(afterReversal.activeTimerStage()).isEqualTo(TimerStage.GAP_RISK);

        long supersedingTimerTimestamp = epochMillis(shrunkEndDate.minusDays(5));
        assertThat(afterReversal.activeTimerTimestamp()).isEqualTo(supersedingTimerTimestamp);
        assertThat(afterReversal.activeTimerTimestamp()).isNotEqualTo(extendedTimerTimestamp);

        assertThat(harness.numEventTimeTimers()).isEqualTo(1); // old timer deleted, exactly one new one registered

        // three registrations so far (two fills, one re-arm) against one live timer, and the
        // reversal matched a real interval — so nothing reached the unmatched-reversal path
        assertThat(function().metrics().count(AdherenceMetricsReporter.TIMERS_REGISTERED)).isEqualTo(3L);
        assertThat(function().metrics().count(AdherenceMetricsReporter.REVERSAL_WITHOUT_ORIGINAL)).isZero();

        // firing the superseding timer produces an alert reflecting the shrunk end date, not the old one
        harness.processWatermark(supersedingTimerTimestamp);
        List<GapRiskAlert> alerts = harness.getSideOutput(AdherenceProcessFunction.GAP_RISK_ALERT_TAG)
                .stream().map(StreamRecord::getValue).toList();
        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).expiresOn()).isEqualTo(shrunkEndDate);
    }

    @Test
    void reversalToZeroCoverageEmitsImmediateCorrectiveLapsedAlertWithNoTimerLeft() throws Exception {
        harness.processBroadcastElement(new AlertLeadTimeUpdate("CHRONIC_CARDIAC|RETAIL", 5), 0L);

        LocalDate fillDate = LocalDate.of(2026, Month.MARCH, 1);
        harness.processElement(
                fillEvent("claim-1", "member-1", "CHRONIC_CARDIAC", fillDate, 30, Channel.RETAIL),
                epochMillis(fillDate));
        assertThat(harness.numEventTimeTimers()).isEqualTo(1);

        // reverse the ONLY fill — zero coverage remains
        LocalDate reversalDate = fillDate.plusDays(2);
        harness.processElement(
                reversalEvent("claim-2", "member-1", "CHRONIC_CARDIAC", reversalDate, "claim-1"),
                epochMillis(reversalDate));

        // no watermark advancement needed — this alert is synchronous, not timer-driven
        List<LapsedAlert> alerts = harness.getSideOutput(AdherenceProcessFunction.LAPSED_ALERT_TAG)
                .stream().map(StreamRecord::getValue).toList();
        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).memberId()).isEqualTo("member-1");
        assertThat(alerts.get(0).drugClass()).isEqualTo("CHRONIC_CARDIAC");
        assertThat(alerts.get(0).lapsedOn()).isEqualTo(reversalDate);
        assertThat(alerts.get(0).emittedAt()).isEqualTo(epochMillis(reversalDate));

        assertThat(harness.numEventTimeTimers()).isZero(); // original gap-risk timer deleted, nothing new registered

        AdherenceState afterReversal = function().currentadherenceState();
        assertThat(afterReversal.currentSupplyEndDate()).isNull();
        assertThat(afterReversal.activeTimerTimestamp()).isNull();
        assertThat(afterReversal.activeTimerStage()).isNull();
        assertThat(function().lapsedAlertsEmittedCount()).isEqualTo(1);
    }

    @Test
    void reversalAfterGapRiskAlertAlreadyEmittedStillSupersedesCorrectly() throws Exception {
        harness.processBroadcastElement(new AlertLeadTimeUpdate("CHRONIC_CARDIAC|RETAIL", 5), 0L);

        LocalDate fillDate = LocalDate.of(2026, Month.MARCH, 1);
        harness.processElement(
                fillEvent("claim-1", "member-1", "CHRONIC_CARDIAC", fillDate, 30, Channel.RETAIL),
                epochMillis(fillDate));

        LocalDate expectedEndDate = fillDate.plusDays(30);
        long gapRiskTimerTimestamp = epochMillis(expectedEndDate.minusDays(5));

        // let the gap-risk timer fire — a GapRiskAlert is emitted, state cascades to LAPSED
        harness.processWatermark(gapRiskTimerTimestamp);
        assertThat(harness.getSideOutput(AdherenceProcessFunction.GAP_RISK_ALERT_TAG)
                .stream().map(StreamRecord::getValue).toList()).hasSize(1);
        assertThat(function().currentadherenceState().activeTimerStage()).isEqualTo(TimerStage.LAPSED);
        assertThat(harness.numEventTimeTimers()).isEqualTo(1); // the cascaded lapsed timer, still pending

        // now the fill gets reversed entirely, before the lapsed timer ever fires
        LocalDate reversalDate = expectedEndDate.minusDays(3);
        harness.processElement(
                reversalEvent("claim-2", "member-1", "CHRONIC_CARDIAC", reversalDate, "claim-1"),
                epochMillis(reversalDate));

        // the pending lapsed timer is cleaned up, and a corrective LapsedAlert supersedes the earlier GapRiskAlert
        assertThat(harness.numEventTimeTimers()).isZero();

        List<LapsedAlert> lapsedAlerts = harness.getSideOutput(AdherenceProcessFunction.LAPSED_ALERT_TAG)
                .stream().map(StreamRecord::getValue).toList();
        assertThat(lapsedAlerts).hasSize(1);
        assertThat(lapsedAlerts.get(0).lapsedOn()).isEqualTo(reversalDate);

        AdherenceState afterReversal = function().currentadherenceState();
        assertThat(afterReversal.currentSupplyEndDate()).isNull();
        assertThat(afterReversal.activeTimerTimestamp()).isNull();
        assertThat(afterReversal.activeTimerStage()).isNull();
    }

    @Test
    void timerRegistrationIsLoggedAtDebugWithClaimIdAndTimestamp() throws Exception {
        harness.processBroadcastElement(new AlertLeadTimeUpdate("CHRONIC_CARDIAC|RETAIL", 5), 0L);
        LocalDate fillDate = LocalDate.of(2026, Month.MARCH, 1);
        long expectedTimerTs = epochMillis(fillDate.plusDays(30).minusDays(5));

        try (LogCapture logs = new LogCapture(AdherenceProcessFunction.class, Level.DEBUG)) {
            harness.processElement(
                    fillEvent("claim-1", "member-1", "CHRONIC_CARDIAC", fillDate, 30, Channel.RETAIL),
                    epochMillis(fillDate));

            assertThat(logs.lines())
                    .anyMatch(line -> line.startsWith("DEBUG")
                            && line.contains("Registered GAP_RISK timer")
                            && line.contains("claimId=claim-1")
                            && line.contains("timerTs=" + expectedTimerTs));
        }
    }

    @Test
    void missingLeadTimeLookupNamesTheKeyThatMissedAndTheFallback() throws Exception {
        LocalDate fillDate = LocalDate.of(2026, Month.MARCH, 1);

        // No broadcast entry at all — the fallback path, and the one a bad ref topic produces
        try (LogCapture logs = new LogCapture(AdherenceProcessFunction.class, Level.DEBUG)) {
            harness.processElement(
                    fillEvent("claim-1", "member-1", "CHRONIC_CARDIAC", fillDate, 30, Channel.RETAIL),
                    epochMillis(fillDate));

            assertThat(logs.lines())
                    .anyMatch(line -> line.startsWith("DEBUG")
                            && line.contains("key=CHRONIC_CARDIAC|RETAIL")
                            && line.contains("default of " + DEFAULT_LEAD_DAYS + " days"));
        }
        assertThat(function().missingLeadTimeCount()).isEqualTo(1L);
    }

    @Test
    void lapsedAlertEmissionIsLoggedWithTheAlertIdThatReachesKafka() throws Exception {
        harness.processBroadcastElement(new AlertLeadTimeUpdate("CHRONIC_CARDIAC|RETAIL", 5), 0L);
        LocalDate fillDate = LocalDate.of(2026, Month.MARCH, 1);
        harness.processElement(
                fillEvent("claim-1", "member-1", "CHRONIC_CARDIAC", fillDate, 30, Channel.RETAIL),
                epochMillis(fillDate));

        // First advance fires GAP_RISK and re-arms the timer at the supply end date;
        // the second advance is the one that reaches the LAPSED stage.
        harness.processWatermark(function().currentadherenceState().activeTimerTimestamp());
        long lapsedTimerTs = function().currentadherenceState().activeTimerTimestamp();

        try (LogCapture logs = new LogCapture(AdherenceProcessFunction.class, Level.DEBUG)) {
            harness.processWatermark(lapsedTimerTs);

            List<LapsedAlert> alerts = harness.getSideOutput(AdherenceProcessFunction.LAPSED_ALERT_TAG)
                    .stream().map(StreamRecord::getValue).toList();
            assertThat(alerts).hasSize(1);
            String alertId = alerts.get(0).alertId();

            assertThat(logs.lines())
                    .anyMatch(line -> line.startsWith("DEBUG")
                            && line.contains("LapsedAlert emitted")
                            && line.contains("alertId=" + alertId));
        }
        assertThat(function().currentadherenceState().activeTimerTimestamp()).isNull();
        assertThat(function().lapsedAlertsEmittedCount()).isEqualTo(1L);
    }

    @Test
    void timerFiringWithNoStateLeftIsIgnoredAndSaysSo() throws Exception {
        harness.processBroadcastElement(new AlertLeadTimeUpdate("CHRONIC_CARDIAC|RETAIL", 5), 0L);
        LocalDate fillDate = LocalDate.of(2026, Month.MARCH, 1);
        harness.processElement(
                fillEvent("claim-1", "member-1", "CHRONIC_CARDIAC", fillDate, 30, Channel.RETAIL),
                epochMillis(fillDate));
        long timerTs = function().currentadherenceState().activeTimerTimestamp();

        // State TTL expiry leaves exactly this: a registered timer whose state is gone.
        function().forceAdherenceStateForTest(null);

        try (LogCapture logs = new LogCapture(AdherenceProcessFunction.class, Level.DEBUG)) {
            harness.processWatermark(timerTs);

            assertThat(logs.lines()).anyMatch(line -> line.contains("reason=no-state"));
        }
        assertThat(harness.getSideOutput(AdherenceProcessFunction.GAP_RISK_ALERT_TAG)).isNullOrEmpty();
    }

    @Test
    void timerFiringWhenStateHasNoActiveTimerIsIgnoredAndSaysSo() throws Exception {
        harness.processBroadcastElement(new AlertLeadTimeUpdate("CHRONIC_CARDIAC|RETAIL", 5), 0L);
        LocalDate fillDate = LocalDate.of(2026, Month.MARCH, 1);
        harness.processElement(
                fillEvent("claim-1", "member-1", "CHRONIC_CARDIAC", fillDate, 30, Channel.RETAIL),
                epochMillis(fillDate));

        AdherenceState current = function().currentadherenceState();
        long timerTs = current.activeTimerTimestamp();
        function().forceAdherenceStateForTest(new AdherenceState(
                current.currentSupplyEndDate(), current.lastFillDate(), current.totalDaysCovered(),
                current.activeCoverageIntervals(), current.alertLeadDays(), null, null));

        try (LogCapture logs = new LogCapture(AdherenceProcessFunction.class, Level.DEBUG)) {
            harness.processWatermark(timerTs);

            assertThat(logs.lines()).anyMatch(line -> line.contains("reason=no-active-timer"));
        }
    }

    @Test
    void duplicateClaimIdIsDroppedAndCounted() throws Exception {
        harness.processBroadcastElement(new AlertLeadTimeUpdate("CHRONIC_CARDIAC|RETAIL", 5), 0L);

        LocalDate fillDate = LocalDate.of(2026, Month.MARCH, 1);
        EnrichedFillEvent fill =
                fillEvent("claim-1", "member-1", "CHRONIC_CARDIAC", fillDate, 30, Channel.RETAIL);

        harness.processElement(fill, epochMillis(fillDate));
        long timerAfterFirstFill = function().currentadherenceState().activeTimerTimestamp();

        // the same claim arrives again — an upstream retry, not a real refill
        harness.processElement(fill, epochMillis(fillDate));

        assertThat(function().metrics().count(AdherenceMetricsReporter.DUPLICATE_CLAIM_ID_DROPPED)).isEqualTo(1L);

        // the real assertion: the duplicate changed nothing. A second registration here would
        // break §4's "at most one timer per key" and inflate coverage with a phantom interval.
        assertThat(function().metrics().count(AdherenceMetricsReporter.TIMERS_REGISTERED)).isEqualTo(1L);
        assertThat(harness.numEventTimeTimers()).isEqualTo(1);
        AdherenceState state = function().currentadherenceState();
        assertThat(state.activeTimerTimestamp()).isEqualTo(timerAfterFirstFill);
        assertThat(state.activeCoverageIntervals()).hasSize(1);
    }

    @Test
    void reversalWithNoMatchingIntervalIsCountedAndLeavesStateAlone() throws Exception {
        harness.processBroadcastElement(new AlertLeadTimeUpdate("CHRONIC_CARDIAC|RETAIL", 5), 0L);

        LocalDate fillDate = LocalDate.of(2026, Month.MARCH, 1);
        harness.processElement(
                fillEvent("claim-1", "member-1", "CHRONIC_CARDIAC", fillDate, 30, Channel.RETAIL),
                epochMillis(fillDate));
        AdherenceState afterFill = function().currentadherenceState();

        // a reversal pointing at a claim this key has never seen — upstream ordering or bad data
        LocalDate reversalDate = fillDate.plusDays(5);
        harness.processElement(
                reversalEvent("claim-2", "member-1", "CHRONIC_CARDIAC", reversalDate, "claim-never-existed"),
                epochMillis(reversalDate));

        assertThat(function().metrics().count(AdherenceMetricsReporter.REVERSAL_WITHOUT_ORIGINAL)).isEqualTo(1L);

        // state and the live timer are untouched — the unmatched reversal is a pure no-op
        assertThat(function().currentadherenceState()).isEqualTo(afterFill);
        assertThat(function().metrics().count(AdherenceMetricsReporter.TIMERS_REGISTERED)).isEqualTo(1L);
        assertThat(harness.numEventTimeTimers()).isEqualTo(1);
    }

    private long epochMillis(LocalDate date) {
        return date.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    }

    private AdherenceProcessFunction function() {
        return (AdherenceProcessFunction) ((CoBroadcastWithKeyedOperator<Tuple2<String, String>, EnrichedFillEvent, AlertLeadTimeUpdate, Void>)
                harness.getOperator()).getUserFunction();
    }

    private EnrichedFillEvent reversalEvent(String claimId, String memberId, String drugClass,
                                            LocalDate reversalDate, String originalClaimId) {
        RxFillEvent event = new RxFillEvent(
                EventType.REVERSAL, claimId, memberId, "NDC-1",
                reversalDate, 0, new BigDecimal("30.00"),
                "pharmacy-1", "rx-1", 2, Channel.RETAIL, originalClaimId);
        return new EnrichedFillEvent(event, drugClass);
    }
}
