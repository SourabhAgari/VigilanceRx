package com.healthcare.rxvigilance.serialization.deadletter;

import com.healthcare.rxvigilance.serialization.util.KafkaCoordinates;
import com.healthcare.rxvigilance.serialization.util.KafkaSourceResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeadLetterSplitFunction<T> extends ProcessFunction<KafkaSourceResult<T>, T> {

    private static final Logger LOG = LoggerFactory.getLogger(DeadLetterSplitFunction.class);

    // A bad schema makes every record a dead letter, and one WARN each would flood Cloud
    // Logging. Sampling is safe here only because the dead-letter topic carries every
    // message with these same coordinates in its headers (#149) — that is the full record,
    // this is the pointer to it.
    private static final long WARN_EVERY = 100L;

    private final OutputTag<KafkaSourceResult<T>> deadLetterTag;

    // Per subtask, reset on restart: a log counter, not a metric. #150 adds deadLetterRecords.
    private transient long deadLetters;

    public DeadLetterSplitFunction(OutputTag<KafkaSourceResult<T>> deadLetterTag) {
        this.deadLetterTag = deadLetterTag;
    }

    @Override
    public void open(Configuration parameters) {
        deadLetters = 0L;
    }

    @Override
    public void processElement(KafkaSourceResult<T> tKafkaSourceResult,
                               ProcessFunction<KafkaSourceResult<T>, T>.Context context, Collector<T> collector) throws Exception {
        if (tKafkaSourceResult.isSuccess()) {
            collector.collect(tKafkaSourceResult.value());
            return;
        } deadLetters++;
        if (deadLetters == 1L || deadLetters % WARN_EVERY == 0L) {
            logDeadLetter(tKafkaSourceResult);
        }
        context.output(deadLetterTag, tKafkaSourceResult);
    }

    /**
     * Values are read into locals first so the log call itself has nothing to evaluate
     * (Sonar S2629). rawBytes is PHI under §9 — the length distinguishes a truncated
     * message from an empty one, and the coordinates say where to read the real thing.
     */
    private void logDeadLetter(KafkaSourceResult<T> result) {
        KafkaCoordinates coordinates = result.coordinates();
        String errorMessage = result.errorMessage();
        byte[] rawBytes = result.rawBytes();
        int rawBytesLength = rawBytes == null ? 0 : rawBytes.length;

        LOG.warn("Dead letter {} on this subtask: coordinates={} rawBytesLength={} error={}. "
                        + "The message and these coordinates are on the dead-letter topic; "
                        + "read the payload from there, not from this log.",
                deadLetters, coordinates, rawBytesLength, errorMessage);
    }
}
