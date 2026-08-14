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
        CoBroadcastWithNonKeyedOperator<RxFillEvent, DrugClassRefUpdate, EnrichedFillEvent> coBroadcastOperator =
                new CoBroadcastWithNonKeyedOperator<>(new ChronicClassFilterFunction(),
                        List.of(ChronicClassFilterFunction.NDC_CLASS_DESCRIPTOR));
        harness = new BroadcastOperatorTestHarness<>(coBroadcastOperator, 1, 1, 0);
        harness.getExecutionConfig().registerTypeWithKryoSerializer(EnrichedFillEvent.class, RecordKryoSerializer.class);
        harness.getExecutionConfig().registerTypeWithKryoSerializer(RxFillEvent.class, RecordKryoSerializer.class);
        harness.open();
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

        harness.processBroadcastElement(new DrugClassRefUpdate(
                "NDC-B", new DrugClassRef("DIABETES", true)), 3L);
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
    void broadcastEntriesLoadedGaugeTracksWhatTheBroadcastApplied() throws Exception {
        // An empty broadcast is the silent failure this gauge exists to expose: every fill gets
        // buffered forever, no counter moves, and the job looks healthy while emitting nothing.
        assertThat(function().metrics().gaugeValue(AdherenceMetricsReporter.BROADCAST_ENTRIES_LOADED)).isZero();

        harness.processBroadcastElement(new DrugClassRefUpdate("NDC-CHRONIC",
                new DrugClassRef("CHRONIC_CARDIAC", true)), 0L);
        harness.processBroadcastElement(new DrugClassRefUpdate("NDC-DIABETES",
                new DrugClassRef("DIABETES", true)), 1L);

        assertThat(function().metrics().gaugeValue(AdherenceMetricsReporter.BROADCAST_ENTRIES_LOADED)).isEqualTo(2L);
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
