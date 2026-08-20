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
import com.healthcare.rxvigilance.serialization.kryo.RecordKryoSerializer;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the medication adherence streaming job.
 *
 * <p>Loads the job configuration, initializes the Flink execution environment,
 * builds the streaming topology, and submits the job for execution.
 */
public class AdherenceJob {
    /**
     * Logger for job lifecycle and operational diagnostics.
     */
    private static final Logger LOG = LoggerFactory.getLogger(AdherenceJob.class);

    /**
     * Initializes and executes the Flink streaming job.
     *
     * <p>The method resolves the job configuration and obtains the
     * {@link StreamExecutionEnvironment}, which provides the execution context
     * used to construct the streaming topology. The constructed topology is
     * then submitted for execution.
     *
     * @param args command-line arguments used to configure the job
     * @throws Exception if configuration loading, topology construction, or
     *                   job execution fails
     */
    public static void main(String[] args) throws Exception {
        JobConfig jobConfig = JobConfig.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        buildTopology(env, jobConfig);
        env.execute("adherence-job");
    }

    public static void buildTopology(StreamExecutionEnvironment env, JobConfig jobConfig) {
        ParameterTool params = jobConfig.getParams();
        KafkaConnectionConfig kafkaConfig = jobConfig.getKafkaConfig();
        WatermarkConfig watermarkConfig = jobConfig.getWatermarkConfig();
        StateBackEndConfig stateBackEndConfig = jobConfig.getStateBackEndConfig();

        int defaultAlertDays = params.getInt("alert.lead.days.default", 7);

        // Runs on the JobManager at submission, so this prints once per job start rather
        // than once per subtask. Named fields, never the whole ParameterTool: it carries
        // KAFKA_SASL_PASSWORD, and a dump would put it in Cloud Logging (§9).
        // Level-guarded so the accessors and params lookups are skipped when INFO is off
        // (Sonar S2629 — arguments are evaluated before the call, regardless of level).
        if (LOG.isInfoEnabled()) {
            LOG.info("Starting adherence-job: brokers={} schemaRegistry={} saslConfigured={} "
                            + "topics=[{}, {}, {}] checkpointIntervalMs={} checkpointMinPauseMs={} "
                            + "tolerableCheckpointFailures={} stateTtlDays={} "
                            + "watermarkOutOfOrderness={} watermarkIdleness={} alertLeadDaysDefault={}",
                    kafkaConfig.brokers(),
                    kafkaConfig.schemaRegistryUrl(),
                    kafkaConfig.hasSaslCredentials(),
                    params.get("kafka.topic.rx-fill-events", "rx-fill-events"),
                    params.get("kafka.topic.ndc-drug-class-ref", "ndc-drug-class-ref"),
                    params.get("kafka.topic.alert-lead-time-ref", "alert-lead-time-ref"),
                    jobConfig.getCheckpointConfig().intervalMs(),
                    jobConfig.getCheckpointConfig().minPauseMs(),
                    jobConfig.getCheckpointConfig().tolerableFailures(),
                    stateBackEndConfig.ttlDays(),
                    watermarkConfig.outOfOrderness(),
                    watermarkConfig.idleness(),
                    defaultAlertDays);
        }
        // explicitly define for enriched fill event
        env.getConfig().registerTypeWithKryoSerializer(EnrichedFillEvent.class, RecordKryoSerializer.class);
        // set default behaviour
        env.getConfig().addDefaultKryoSerializer(Record.class, RecordKryoSerializer.class);

        StateBackEndConfig.configureRocksDbBackEnd(env);

        env.enableCheckpointing(jobConfig.getCheckpointConfig().intervalMs());
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(jobConfig.getCheckpointConfig().minPauseMs());
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(jobConfig.getCheckpointConfig().tolerableFailures());
        String checkpointDirectory = jobConfig.getCheckpointConfig().checkpointDirectory();
        if (checkpointDirectory != null) {
            env.getCheckpointConfig().setCheckpointStorage(checkpointDirectory);
            LOG.info("Checkpoint storage set from checkpoint.dir: {}", checkpointDirectory);
        } else {
            // Running under the Flink operator: state.checkpoints.dir comes from
            // the cluster configuration in flink-deployment.yaml, and Flink applies
            // it without being told. Setting it here as well is what put the same
            // path in two files that had to agree (#132).
            LOG.info("checkpoint.dir not set - relying on the cluster's "
                    + "state.checkpoints.dir. If that is unset too, Flink keeps "
                    + "checkpoints in JobManager memory and they are lost on failure.");
        }

        RxFillEventSource.RxFillEventSourceResult fillEventSourceResult =
                RxFillEventSource.build(env,kafkaConfig,watermarkConfig,params);
        DataStream<RxFillEvent> fillEvents = fillEventSourceResult.events();

        DrugClassRefKafkaSource.DrugClassRefSourceResult drugClassRefResult =
                DrugClassRefKafkaSource.build(env, kafkaConfig, watermarkConfig, params);
        SingleOutputStreamOperator<DrugClassRefUpdate> drugClassUpdates = drugClassRefResult.events();

        AlertLeadTimeKafkaSource.AlertLeadRefSourceResult leadTimeResult =
                AlertLeadTimeKafkaSource.build(env, kafkaConfig, watermarkConfig, params);
        SingleOutputStreamOperator<AlertLeadTimeUpdate> leadTimeUpdates = leadTimeResult.events();

        BroadcastStream<DrugClassRefUpdate> drugClassBroadcast =
                drugClassUpdates.broadcast(ChronicClassFilterFunction.NDC_CLASS_DESCRIPTOR);

        SingleOutputStreamOperator<EnrichedFillEvent> enrichedFillEvents = fillEvents
                .connect(drugClassBroadcast)
                .process(new ChronicClassFilterFunction())
                .name("chronic-class-filter")
                .uid("chronic-class-filter");

        KeyedStream<EnrichedFillEvent, Tuple2<String, String>> keyedFillEvents = enrichedFillEvents
                .keyBy(e -> Tuple2.of(e.event().memberId(), e.drugClass()),
                        Types.TUPLE(Types.STRING, Types.STRING));

        BroadcastStream<AlertLeadTimeUpdate> leadTimeBroadcast =
                leadTimeUpdates.broadcast(AdherenceProcessFunction.LEAD_TIME_DESCRIPTOR);

        SingleOutputStreamOperator<Void> adherenceResults = keyedFillEvents
                .connect(leadTimeBroadcast)
                .process(new AdherenceProcessFunction(stateBackEndConfig, defaultAlertDays))
                .name("adherence-process")
                .uid("adherence-process");

        adherenceResults.getSideOutput(AdherenceProcessFunction.GAP_RISK_ALERT_TAG)
                .sinkTo(AlertKafkaSinks.gapRiskAlertSink(env, kafkaConfig, params))
                .name("gap-risk-alerts-sink")
                .uid("gap-risk-alerts-sink");

        adherenceResults.getSideOutput(AdherenceProcessFunction.LAPSED_ALERT_TAG)
                .sinkTo(AlertKafkaSinks.lapsedAlertSink(env, kafkaConfig, params))
                .name("lapsed-alerts-sink")
                .uid("lapsed-alerts-sink");

        adherenceResults.getSideOutput(AdherenceProcessFunction.PDC_SNAPSHOT_OUTPUT_TAG)
                .sinkTo(AlertKafkaSinks.pdcSnapshotSink(env, kafkaConfig, params))
                .name("pdc-snapshots-sink")
                .uid("pdc-snapshots-sink");

        DataStream<DeadLetterRecord> fillEventDeadLetters =
                fillEventSourceResult.deadLetters().map(DeadLetterRecord::from)
                        .name("rx-fill-events-dead-letter-record")
                        .uid("rx-fill-events-dead-letter-record");
        DataStream<DeadLetterRecord> drugClassDeadLetters =
                drugClassRefResult.deadLetters().map(DeadLetterRecord::from)
                        .name("ndc-drug-class-ref-dead-letter-record")
                        .uid("ndc-drug-class-ref-dead-letter-record");
        DataStream<DeadLetterRecord> leadTimeDeadLetters =
                leadTimeResult.deadLetters().map(DeadLetterRecord::from)
                        .name("alert-lead-time-ref-dead-letter-record")
                        .uid("alert-lead-time-ref-dead-letter-record");

        fillEventDeadLetters.union(drugClassDeadLetters, leadTimeDeadLetters)
                .sinkTo(AlertKafkaSinks.deadLetterSink(env, kafkaConfig, params))
                .name("dead-letter-sink")
                .uid("dead-letter-sink");
    }
}
