package com.healthcare.rxvigilance.pipeline.operators;

import com.healthcare.rxvigilance.domain.DrugClassRef;
import com.healthcare.rxvigilance.domain.DrugClassRefUpdate;
import com.healthcare.rxvigilance.domain.EnrichedFillEvent;
import com.healthcare.rxvigilance.domain.RxFillEvent;
import com.healthcare.rxvigilance.domain.enums.Channel;
import com.healthcare.rxvigilance.domain.enums.EventType;
import com.healthcare.rxvigilance.logging.LogCapture;
import com.healthcare.rxvigilance.metrics.AdherenceMetricsReporter;
import com.healthcare.rxvigilance.serialization.kryo.RecordKryoSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithNonKeyedOperator;
import org.apache.flink.streaming.util.BroadcastOperatorTestHarness;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.Month;
import java.util.List;


import static org.assertj.core.api.Assertions.assertThat;

class ChronicClassFilterFunctionTest {

    private BroadcastOperatorTestHarness<RxFillEvent, DrugClassRefUpdate, EnrichedFillEvent> harness;

    @BeforeEach
    void setUp() throws Exception {
        harness = newHarness();
        harness.open();
    }

    /**
     * A fresh harness over a fresh function, with the same Kryo registrations. Extracted so the
     * restore test can build a second one — the operator has to be new for a restore to prove
     * anything.
     */
    private static BroadcastOperatorTestHarness<RxFillEvent, DrugClassRefUpdate, EnrichedFillEvent> newHarness() throws Exception {
        CoBroadcastWithNonKeyedOperator<RxFillEvent, DrugClassRefUpdate, EnrichedFillEvent> coBroadcastOperator =
                new CoBroadcastWithNonKeyedOperator<>(new ChronicClassFilterFunction(),
                        List.of(ChronicClassFilterFunction.NDC_CLASS_DESCRIPTOR));
        BroadcastOperatorTestHarness<RxFillEvent, DrugClassRefUpdate, EnrichedFillEvent> created =
                new BroadcastOperatorTestHarness<>(coBroadcastOperator, 1, 1, 0);
        created.getExecutionConfig().registerTypeWithKryoSerializer(EnrichedFillEvent.class, RecordKryoSerializer.class);
        created.getExecutionConfig().registerTypeWithKryoSerializer(RxFillEvent.class, RecordKryoSerializer.class);
        return created;
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    @Test
    void acuteDrugDiscarded() throws Exception {
        harness.processBroadcastElement(new DrugClassRefUpdate("NDC-ACUTE",
                new DrugClassRef("NDC-ACUTE", false)), 0L);
        harness.processElement(fillEvent("NDC-ACUTE", 0), 1L);

        assertThat(harness.extractOutputValues()).isEmpty();
        assertThat(function().droppedCount()).isEqualTo(1);
    }

    @Test
    void chronicWithZeroRefillsAuthorizedIsKept() throws Exception {
        harness.processBroadcastElement(new DrugClassRefUpdate("NDC-CHRONIC",
                new DrugClassRef("CHRONIC_CARDIAC", true)), 0L);
        RxFillEvent event = fillEvent("NDC-CHRONIC", 0);
        harness.processElement(fillEvent("NDC-CHRONIC", 0), 1L);

        assertThat(harness.extractOutputValues()).isNotEmpty();
        assertThat(harness.extractOutputValues()).hasSize(1);
        assertThat(harness.extractOutputValues()).containsExactly(new EnrichedFillEvent(event, "CHRONIC_CARDIAC"));
        assertThat(function().droppedCount()).isZero();
    }

    @Test
    void diabetesClassesSpecialtyDispensedNdcDiscarded() throws Exception {
        harness.processBroadcastElement(
                new DrugClassRefUpdate("NDC-DIABETES-SPECIALTY", new DrugClassRef("DIABETES", false)), 0L);
        harness.processElement(fillEvent("NDC-DIABETES-SPECIALTY", 2), 1L);

        assertThat(harness.extractOutputValues()).isEmpty();
        assertThat(function().droppedCount()).isEqualTo(1L);
    }

    @Test
    void eventArrivingBeforeFirstBroadcastUpdateIsBufferedThenReplayed() throws Exception {
        RxFillEvent event = fillEvent("NDC-CHRONIC", 1);
        harness.processElement(event, 0L);

        assertThat(harness.extractOutputValues()).isEmpty();

        harness.processBroadcastElement(new DrugClassRefUpdate(
                "NDC-CHRONIC", new DrugClassRef("CHRONIC_CARDIAC", true)
        ), 1L);
        assertThat(harness.extractOutputValues())
                .containsExactly(new EnrichedFillEvent(event, "CHRONIC_CARDIAC"));
    }

    @Test
    void eventForNewNdcArrivingWhileMapAlreadyPopulatedIsBufferedThenReplayed() throws Exception {
        // map is non-empty from the start — this is what isEmpty() missed
        harness.processBroadcastElement(new DrugClassRefUpdate(
                "NDC-EXISTING", new DrugClassRef("CHRONIC_CARDIAC", true)), 0L);

        RxFillEvent event = fillEvent("NDC-NEW", 1);
        harness.processElement(event, 1L);

        assertThat(harness.extractOutputValues()).isEmpty(); // must NOT be dropped yet

        harness.processBroadcastElement(new DrugClassRefUpdate(
                "NDC-NEW", new DrugClassRef("DIABETES", true)), 2L);

        assertThat(harness.extractOutputValues())
                .containsExactly(new EnrichedFillEvent(event, "DIABETES"));
        assertThat(function().droppedCount()).isZero();
    }

    @Test
    void unrelatedBroadcastUpdateDoesNotDropOtherBufferedEvents() throws Exception {
        RxFillEvent eventA = fillEvent("NDC-A", 1);
        RxFillEvent eventB = fillEvent("NDC-B", 1);
        harness.processElement(eventA, 0L);
        harness.processElement(eventB, 1L);

        // resolves NDC-A only — NDC-B's event must survive in the buffer
        harness.processBroadcastElement(new DrugClassRefUpdate(
                "NDC-A", new DrugClassRef("CHRONIC_CARDIAC", true)), 2L);
        assertThat(harness.extractOutputValues())
                .containsExactly(new EnrichedFillEvent(eventA, "CHRONIC_CARDIAC"));
        assertThat(function().metrics().gaugeValue(AdherenceMetricsReporter.BUFFERED_FILLS_AWAITING_REF)).isEqualTo(1L);

        harness.processBroadcastElement(new DrugClassRefUpdate(
                "NDC-B", new DrugClassRef("DIABETES", true)), 3L);
        assertThat(function().metrics().gaugeValue(AdherenceMetricsReporter.BUFFERED_FILLS_AWAITING_REF)).isZero();
        assertThat(harness.extractOutputValues())
                .containsExactly(
                        new EnrichedFillEvent(eventA, "CHRONIC_CARDIAC"),
                        new EnrichedFillEvent(eventB, "DIABETES"));

    }

    @Test
    void broadcastEntryCountIsLoggedSoAnEmptyBroadcastIsVisible() throws Exception {
        try (LogCapture logs = new LogCapture(ChronicClassFilterFunction.class, Level.INFO)) {
            harness.processBroadcastElement(new DrugClassRefUpdate("NDC-CHRONIC",
                    new DrugClassRef("CHRONIC_CARDIAC", true)), 0L);

            assertThat(logs.lines())
                    .anyMatch(line -> line.startsWith("INFO") && line.contains("applied 1 entries"));
        }
    }

    @Test
    void bufferingAnEventWithoutItsDrugClassRefIsLoggedAtDebugWithClaimAndNdc() throws Exception {
        try (LogCapture logs = new LogCapture(ChronicClassFilterFunction.class, Level.DEBUG)) {
            harness.processElement(fillEvent("NDC-UNKNOWN", 0), 0L);

            assertThat(logs.lines())
                    .anyMatch(line -> line.startsWith("DEBUG")
                            && line.contains("claimId=claim-1")
                            && line.contains("ndcCode=NDC-UNKNOWN"));
        }
    }

    @Test
    void releasingBufferedEventsIsLoggedWithHowManyWereReleased() throws Exception {
        harness.processElement(fillEvent("NDC-CHRONIC", 0), 0L);
        harness.processElement(fillEvent("NDC-CHRONIC", 0), 1L);

        try (LogCapture logs = new LogCapture(ChronicClassFilterFunction.class, Level.DEBUG)) {
            harness.processBroadcastElement(new DrugClassRefUpdate("NDC-CHRONIC",
                    new DrugClassRef("CHRONIC_CARDIAC", true)), 2L);

            assertThat(logs.lines())
                    .anyMatch(line -> line.startsWith("DEBUG")
                            && line.contains("ndcCode=NDC-CHRONIC")
                            && line.contains("released=2"));
        }
        assertThat(harness.extractOutputValues()).hasSize(2);
    }

    @Test
    void bufferedFillsGaugeRisesWhileWaitingForTheRefAndFallsWhenItArrives() throws Exception {
        assertThat(function().metrics().gaugeValue(AdherenceMetricsReporter.BUFFERED_FILLS_AWAITING_REF)).isZero();

        // no drug-class ref for NDC-CHRONIC yet, so both fills park in the buffer
        harness.processElement(fillEvent("NDC-CHRONIC", 0), 0L);
        harness.processElement(fillEvent("NDC-CHRONIC", 0), 1L);
        assertThat(function().metrics().gaugeValue(AdherenceMetricsReporter.BUFFERED_FILLS_AWAITING_REF)).isEqualTo(2L);

        harness.processBroadcastElement(new DrugClassRefUpdate("NDC-CHRONIC",
                new DrugClassRef("CHRONIC_CARDIAC", true)), 2L);

        assertThat(function().metrics().gaugeValue(AdherenceMetricsReporter.BUFFERED_FILLS_AWAITING_REF)).isZero();
        assertThat(harness.extractOutputValues()).hasSize(2);
    }

    /**
     * The regression test for #175. The old broadcastEntriesLoaded gauge counted arrivals, so it
     * read 0 here — the buffer comes back from the snapshot without a single call to
     * processElement or processBroadcastElement to count it. Nothing flows in this test on
     * purpose; that is the whole point.
     */
    @Test
    void bufferedFillsGaugeIsCorrectAfterARestoreWithNothingFlowing() throws Exception {
        harness.processElement(fillEvent("NDC-CHRONIC", 0), 0L);
        harness.processElement(fillEvent("NDC-CHRONIC", 0), 1L);
        assertThat(function().metrics().gaugeValue(AdherenceMetricsReporter.BUFFERED_FILLS_AWAITING_REF)).isEqualTo(2L);

        OperatorSubtaskState snapshot = harness.snapshot(1L, 1L);
        harness.close();

        harness = newHarness();   // reassigned so @AfterEach closes exactly one open harness
        harness.initializeState(snapshot);
        harness.open();

        assertThat(function().metrics().gaugeValue(AdherenceMetricsReporter.BUFFERED_FILLS_AWAITING_REF)).isEqualTo(2L);

        // and the count agrees with what the buffer actually holds — the ref drains both
        harness.processBroadcastElement(new DrugClassRefUpdate("NDC-CHRONIC",
                new DrugClassRef("CHRONIC_CARDIAC", true)), 2L);
        assertThat(harness.extractOutputValues()).hasSize(2);
        assertThat(function().metrics().gaugeValue(AdherenceMetricsReporter.BUFFERED_FILLS_AWAITING_REF)).isZero();
    }

    private RxFillEvent fillEvent(String ndcCode, int refillsAuthorized) {
        return new RxFillEvent(
                EventType.FILL, "claim-1", "member-1", ndcCode,
                LocalDate.of(2026, Month.MARCH, 1), 30, new BigDecimal("30.00"),
                "pharmacy-1", "rx-1", refillsAuthorized, Channel.RETAIL, null);
    }

    @SuppressWarnings("unchecked")
    private ChronicClassFilterFunction function() {
        return (ChronicClassFilterFunction) ((CoBroadcastWithNonKeyedOperator<RxFillEvent, DrugClassRefUpdate, EnrichedFillEvent>)
                harness.getOperator()).getUserFunction();
    }
}
