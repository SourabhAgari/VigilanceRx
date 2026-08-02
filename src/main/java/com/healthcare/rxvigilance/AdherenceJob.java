package com.healthcare.rxvigilance;

import com.healthcare.rxvigilance.config.JobConfig;
import com.healthcare.rxvigilance.config.KafkaConnectionConfig;
import com.healthcare.rxvigilance.config.StateBackEndConfig;
import com.healthcare.rxvigilance.config.WatermarkConfig;
import com.healthcare.rxvigilance.domain.AlertLeadTimeUpdate;
import com.healthcare.rxvigilance.domain.DrugClassRefUpdate;
import com.healthcare.rxvigilance.domain.EnrichedFillEvent;
import com.healthcare.rxvigilance.domain.RxFillEvent;
import com.healthcare.rxvigilance.pipeline.operators.AdherenceProcessFunction;
import com.healthcare.rxvigilance.pipeline.operators.ChronicClassFilterFunction;
import com.healthcare.rxvigilance.pipeline.sink.AlertKafkaSinks;
import com.healthcare.rxvigilance.pipeline.source.AlertLeadTimeKafkaSource;
import com.healthcare.rxvigilance.pipeline.source.DrugClassRefKafkaSource;
import com.healthcare.rxvigilance.pipeline.source.RxFillEventSource;
import com.healthcare.rxvigilance.serialization.deadletter.DeadLetterRecord;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AdherenceJob {
    public static void main(String[] args) throws Exception {
        JobConfig jobConfig = JobConfig.fromArgs(args);
        ParameterTool params = jobConfig.getParams();
        KafkaConnectionConfig kafkaConfig = jobConfig.getKafkaConfig();
        WatermarkConfig watermarkConfig = jobConfig.getWatermarkConfig();
        StateBackEndConfig stateBackEndConfig = jobConfig.getStateBackEndConfig();

        int defaultAlertDays = params.getInt("alert.lead.days.default",7);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StateBackEndConfig.configureRocksDbBackEnd(env);

        env.enableCheckpointing(jobConfig.getCheckpointConfig().intervalMs());
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(jobConfig.getCheckpointConfig().minPauseMs());
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(jobConfig.getCheckpointConfig().tolerableFailures());
        env.getCheckpointConfig().setCheckpointStorage(jobConfig.getCheckpointConfig().checkpointDirectory());

        RxFillEventSource.RxFillEventSourceResult fillEventSourceResult =
                RxFillEventSource.build(env,kafkaConfig,watermarkConfig,params);
        DataStream<RxFillEvent> fillEvents = fillEventSourceResult.events();

        SingleOutputStreamOperator<DrugClassRefUpdate> drugClassUpdates =
                DrugClassRefKafkaSource.build(env, kafkaConfig, params);
        SingleOutputStreamOperator<AlertLeadTimeUpdate> leadTimeUpdates =
                AlertLeadTimeKafkaSource.build(env, kafkaConfig, params);

        BroadcastStream<DrugClassRefUpdate> drugClassBroadcast =
                drugClassUpdates.broadcast(ChronicClassFilterFunction.NDC_CLASS_DESCRIPTOR);

        SingleOutputStreamOperator<EnrichedFillEvent> enrichedFillEvents = fillEvents
                .connect(drugClassBroadcast)
                .process(new ChronicClassFilterFunction())
                .uid("chronic-class-filter");

        KeyedStream<EnrichedFillEvent, Tuple2<String, String>> keyedFillEvents = enrichedFillEvents
                .keyBy(e -> Tuple2.of(e.event().memberId(), e.drugClass()),
                        Types.TUPLE(Types.STRING, Types.STRING));

        BroadcastStream<AlertLeadTimeUpdate> leadTimeBroadcast =
                leadTimeUpdates.broadcast(AdherenceProcessFunction.LEAD_TIME_DESCRIPTOR);

        SingleOutputStreamOperator<Void> adherenceResults = keyedFillEvents
                .connect(leadTimeBroadcast)
                .process(new AdherenceProcessFunction(stateBackEndConfig, defaultAlertDays))
                .uid("adherence-process");

        adherenceResults.getSideOutput(AdherenceProcessFunction.GAP_RISK_ALERT_TAG)
                .sinkTo(AlertKafkaSinks.gapRiskAlertSink(env, kafkaConfig, params))
                .uid("gap-risk-alerts-sink");

        adherenceResults.getSideOutput(AdherenceProcessFunction.LAPSED_ALERT_TAG)
                .sinkTo(AlertKafkaSinks.lapsedAlertSink(env, kafkaConfig, params))
                .uid("lapsed-alerts-sink");

        adherenceResults.getSideOutput(AdherenceProcessFunction.PDC_SNAPSHOT_OUTPUT_TAG)
                .sinkTo(AlertKafkaSinks.pdcSnapshotSink(env, kafkaConfig, params))
                .uid("pdc-snapshots-sink");

        DataStream<DeadLetterRecord> fillEventDeadLetters =
                fillEventSourceResult.deadLetters().map(DeadLetterRecord::from);
        DataStream<DeadLetterRecord> drugClassDeadLetters =
                drugClassUpdates.getSideOutput(DrugClassRefKafkaSource.DEAD_LETTER_TAG).map(DeadLetterRecord::from);
        DataStream<DeadLetterRecord> leadTimeDeadLetters =
                leadTimeUpdates.getSideOutput(AlertLeadTimeKafkaSource.DEAD_LETTER_TAG).map(DeadLetterRecord::from);

        fillEventDeadLetters.union(drugClassDeadLetters, leadTimeDeadLetters)
                .sinkTo(AlertKafkaSinks.deadLetterSink(env, kafkaConfig, params))
                .uid("dead-letter-sink");

        env.execute("adherence-job");
    }
}
