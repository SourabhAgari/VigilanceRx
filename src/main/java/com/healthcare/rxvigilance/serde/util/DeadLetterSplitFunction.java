package com.healthcare.rxvigilance.serde.util;

import com.healthcare.rxvigilance.serde.KafkaSourceResult;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DeadLetterSplitFunction<T> extends ProcessFunction<KafkaSourceResult<T>, T> {

    private final OutputTag<KafkaSourceResult<T>> deadLetterTag;

    public DeadLetterSplitFunction(OutputTag<KafkaSourceResult<T>> deadLetterTag) {
        this.deadLetterTag = deadLetterTag;
    }

    @Override
    public void processElement(KafkaSourceResult<T> tKafkaSourceResult,
                               ProcessFunction<KafkaSourceResult<T>, T>.Context context, Collector<T> collector) throws Exception {
        if (tKafkaSourceResult.isSuccess()) {
            collector.collect(tKafkaSourceResult.value());
        } else {
            context.output(deadLetterTag, tKafkaSourceResult);
        }
    }
}
