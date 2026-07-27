package com.healthcare.rxvigilance.serialization.util;

import com.healthcare.rxvigilance.serialization.KafkaSourceResult;
import com.healthcare.rxvigilance.serialization.kryo.RecordKryoSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.util.OutputTag;
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
}
