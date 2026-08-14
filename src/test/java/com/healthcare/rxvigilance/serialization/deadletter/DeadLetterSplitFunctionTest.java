package com.healthcare.rxvigilance.serialization.deadletter;

import com.healthcare.rxvigilance.logging.LogCapture;
import com.healthcare.rxvigilance.serialization.kryo.RecordKryoSerializer;
import com.healthcare.rxvigilance.serialization.util.KafkaCoordinates;
import com.healthcare.rxvigilance.serialization.util.KafkaSourceResult;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.util.OutputTag;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DeadLetterSplitFunctionTest {
    private static final OutputTag<KafkaSourceResult<String>> DEAD_LETTER_TAG =
            new OutputTag<>("test-dead-letter") { };

    @Test
    void successfulResultIsCollectedToMainOutput() throws Exception {
        OneInputStreamOperatorTestHarness<KafkaSourceResult<String>, String> harness =
                ProcessFunctionTestHarnesses.forProcessFunction(new DeadLetterSplitFunction<>(DEAD_LETTER_TAG));
        harness.getExecutionConfig().registerTypeWithKryoSerializer(KafkaSourceResult.class, RecordKryoSerializer.class);

        harness.processElement(KafkaSourceResult.success("hello"), 0L);

        assertThat(harness.getOutput())
                .extracting(o -> ((StreamRecord<String>) o).getValue())
                .containsExactly("hello");
    }

    @Test
    void failedResultIsRoutedToDeadLetterSideOutput() throws Exception {
        OneInputStreamOperatorTestHarness<KafkaSourceResult<String>, String> harness =
                ProcessFunctionTestHarnesses.forProcessFunction(new DeadLetterSplitFunction<>(DEAD_LETTER_TAG));
        harness.getExecutionConfig().registerTypeWithKryoSerializer(KafkaSourceResult.class, RecordKryoSerializer.class);

        KafkaSourceResult<String> failure = KafkaSourceResult.failure(new byte[]{1, 2, 3}, "bad bytes");
        harness.processElement(failure, 0L);

        assertThat(harness.getOutput()).isEmpty();
        assertThat(harness.getSideOutput(DEAD_LETTER_TAG))
                .extracting(StreamRecord::getValue)
                .containsExactly(failure);
    }

    @Test
    void deadLetterIsLoggedAtWarnWithItsCoordinatesButNeverItsPayload() throws Exception {
        OneInputStreamOperatorTestHarness<KafkaSourceResult<String>, String> harness =
                ProcessFunctionTestHarnesses.forProcessFunction(new DeadLetterSplitFunction<>(DEAD_LETTER_TAG));
        harness.getExecutionConfig().registerTypeWithKryoSerializer(KafkaSourceResult.class, RecordKryoSerializer.class);

        KafkaSourceResult<String> failure = KafkaSourceResult.<String>failure(new byte[]{1, 2, 3}, "bad bytes")
                .withCoordinates(new KafkaCoordinates("rx-fill-events", 2, 884211L, 1_700_000_000_000L));

        try (LogCapture logs = new LogCapture(DeadLetterSplitFunction.class, Level.WARN)) {
            harness.processElement(failure, 0L);

            assertThat(logs.lines())
                    .anyMatch(line -> line.startsWith("WARN")
                            && line.contains("rx-fill-events")
                            && line.contains("884211")
                            && line.contains("bad bytes")
                            && line.contains("rawBytesLength=3"))
                    .noneMatch(line -> line.contains("[1, 2, 3]"));
        }
    }

    @Test
    void deadLetterWarningsAreSampledAfterTheFirst() throws Exception {
        OneInputStreamOperatorTestHarness<KafkaSourceResult<String>, String> harness =
                ProcessFunctionTestHarnesses.forProcessFunction(new DeadLetterSplitFunction<>(DEAD_LETTER_TAG));
        harness.getExecutionConfig().registerTypeWithKryoSerializer(KafkaSourceResult.class, RecordKryoSerializer.class);

        try (LogCapture logs = new LogCapture(DeadLetterSplitFunction.class, Level.WARN)) {
            for (int i = 0; i < 100; i++) {
                harness.processElement(KafkaSourceResult.<String>failure(new byte[]{1}, "bad bytes"), i);
            }

            // First and hundredth only — a poison-pill storm must not become 100 log lines
            assertThat(logs.lines()).hasSize(2);
        }
        assertThat(harness.getSideOutput(DEAD_LETTER_TAG)).hasSize(100);
    }

    @SuppressWarnings("unchecked")
    private static DeadLetterSplitFunction<String> function(
            OneInputStreamOperatorTestHarness<KafkaSourceResult<String>, String> harness) {
        return (DeadLetterSplitFunction<String>)
                ((ProcessOperator<KafkaSourceResult<String>, String>) harness.getOperator()).getUserFunction();
    }

}
