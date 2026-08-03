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

import java.util.ArrayList;
import java.util.List;

public class ChronicClassFilterFunction
        extends BroadcastProcessFunction<RxFillEvent, DrugClassRefUpdate, EnrichedFillEvent>
        implements CheckpointedFunction {

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
            return;
        }
        collector.collect(new EnrichedFillEvent(event,ref.drugClass()));
    }

    public void processBroadcastElement(DrugClassRefUpdate drugClassRefUpdate, Context context, Collector<EnrichedFillEvent> collector) throws Exception {
        BroadcastState<String, DrugClassRef> broadcastState = context.getBroadcastState(NDC_CLASS_DESCRIPTOR);
        broadcastState.put(drugClassRefUpdate.ndcCode(), drugClassRefUpdate.drugClassRef());

        List<RxFillEvent> remaining = new ArrayList<>();
        for (RxFillEvent buffered : bufferState.get()) {
            if (buffered.ndcCode().equals(drugClassRefUpdate.ndcCode())) {
                filterAndEmit(buffered, broadcastState, collector);
            } else {
                remaining.add(buffered);
            }
        }
        bufferState.clear();
        for (RxFillEvent r : remaining) {
            bufferState.add(r);
        }
    }

    public long droppedCount() {
        return droppedCounter.getCount();
    }
}
