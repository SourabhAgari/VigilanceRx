package com.healthcare.rxvigilance.pipeline.operators;

import com.healthcare.rxvigilance.domain.DrugClassRef;
import com.healthcare.rxvigilance.domain.DrugClassRefUpdate;
import com.healthcare.rxvigilance.domain.EnrichedFillEvent;
import com.healthcare.rxvigilance.domain.RxFillEvent;
import com.healthcare.rxvigilance.domain.enums.Channel;
import com.healthcare.rxvigilance.domain.enums.EventType;
import com.healthcare.rxvigilance.serialization.kryo.RecordKryoSerializer;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithNonKeyedOperator;
import org.apache.flink.streaming.util.BroadcastOperatorTestHarness;
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
