package com.healthcare.rxvigilance.pipeline.operators;

import com.healthcare.rxvigilance.domain.DrugClassRef;
import com.healthcare.rxvigilance.domain.DrugClassRefUpdate;
import com.healthcare.rxvigilance.domain.EnrichedFillEvent;
import com.healthcare.rxvigilance.domain.RxFillEvent;
import com.healthcare.rxvigilance.metrics.AdherenceMetricsReporter;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ChronicClassFilterFunction
        extends BroadcastProcessFunction<RxFillEvent, DrugClassRefUpdate, EnrichedFillEvent>
        implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(ChronicClassFilterFunction.class);

    // Ref tables run to thousands of rows; one INFO per row would bury the startup log.
    // The first entry and every 500th are enough to tell "loading" from "never arrived".
    private static final long BROADCAST_LOG_EVERY = 500L;

    // Per subtask and reset on restart: a log counter, not a metric. #150 adds the metric.
    private transient long broadcastEntriesApplied;

    public static final MapStateDescriptor<String, DrugClassRef> NDC_CLASS_DESCRIPTOR =
            new MapStateDescriptor<>("ndc-class-state", Types.STRING, TypeInformation.of(DrugClassRef.class));
    private static final ListStateDescriptor<RxFillEvent> BUFFER_DESCRIPTOR =
            new ListStateDescriptor<>("pre-broadcast-buffer",TypeInformation.of(RxFillEvent.class));

    private transient Counter droppedCounter;
    private transient ListState<RxFillEvent> bufferState;

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        bufferState = context.getOperatorStateStore().getListState(BUFFER_DESCRIPTOR);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // ListState persists incrementally via add()/clear() below; no separate sync needed.
    }

    @Override
    public void open(Configuration parameters) {
        droppedCounter = AdherenceMetricsReporter.register(getRuntimeContext()).chronicFilterDropped();
        broadcastEntriesApplied = 0L;
    }

    @Override
    public void processElement(RxFillEvent event,
                               BroadcastProcessFunction<RxFillEvent, DrugClassRefUpdate,
                                       EnrichedFillEvent>.ReadOnlyContext readOnlyContext,
                               Collector<EnrichedFillEvent> collector) throws Exception {
        ReadOnlyBroadcastState<String,DrugClassRef> broadcastState = readOnlyContext.getBroadcastState(NDC_CLASS_DESCRIPTOR);
        DrugClassRef ref = broadcastState.get(event.ndcCode());
        if (ref == null) {
            bufferState.add(event);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Buffering fill event until its drug class ref arrives: claimId={} ndcCode={}",
                        event.claimId(), event.ndcCode());
            }
            return;
        }
        filterAndEmit(event,broadcastState,collector);
    }

    private void filterAndEmit(RxFillEvent event,
                               ReadOnlyBroadcastState<String, DrugClassRef> broadcastState,
                               Collector<EnrichedFillEvent> collector) throws Exception {
        DrugClassRef ref = broadcastState.get(event.ndcCode());
        if(ref == null || !ref.trackable()) {
            droppedCounter.inc();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Dropping fill event: claimId={} ndcCode={} reason={}",
                        event.claimId(), event.ndcCode(),
                        ref == null ? "no-drug-class-ref" : "not-trackable");
            }
            return;
        }
        collector.collect(new EnrichedFillEvent(event,ref.drugClass()));
    }

    public void processBroadcastElement(DrugClassRefUpdate drugClassRefUpdate, Context context, Collector<EnrichedFillEvent> collector) throws Exception {
        BroadcastState<String, DrugClassRef> broadcastState = context.getBroadcastState(NDC_CLASS_DESCRIPTOR);
        broadcastState.put(drugClassRefUpdate.ndcCode(), drugClassRefUpdate.drugClassRef());

        broadcastEntriesApplied++;
        if (broadcastEntriesApplied == 1L || broadcastEntriesApplied % BROADCAST_LOG_EVERY == 0L) {
            LOG.info("Drug class broadcast applied {} entries on this subtask", broadcastEntriesApplied);
        }

        List<RxFillEvent> remaining = new ArrayList<>();
        int released = 0;
        for (RxFillEvent buffered : bufferState.get()) {
            if (buffered.ndcCode().equals(drugClassRefUpdate.ndcCode())) {
                filterAndEmit(buffered, broadcastState, collector);
                released++;
            } else {
                remaining.add(buffered);
            }
        }
        bufferState.clear();
        for (RxFillEvent r : remaining) {
            bufferState.add(r);
        }

        if(released > 0 && LOG.isDebugEnabled()) {
            LOG.debug("Released buffered fill events after drug class ref arrived: ndcCode={} released={}",
                    drugClassRefUpdate.ndcCode(), released);
        }
    }

    public long droppedCount() {
        return droppedCounter.getCount();
    }
}
