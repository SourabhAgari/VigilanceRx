package com.healthcare.rxvigilance;

import com.healthcare.rxvigilance.config.JobConfig;
import com.healthcare.rxvigilance.pipeline.operators.AdherenceProcessFunction;
import com.healthcare.rxvigilance.pipeline.operators.ChronicClassFilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;


class AdherenceJobTopologyTest  {

    @TempDir
    Path tempDir;

    /**
     * buildTopology only describes the pipeline — no Kafka connection is opened until the job is
     * executed. So the graph can be built and inspected with placeholder broker addresses.
     */
    private StreamGraph buildGraph() throws Exception {
        JobConfig jobConfig = JobConfig.fromArgs(new String[]{
                "--kafka.brokers", "localhost:9092",
                "--schema.registry.url", "http://localhost:8081",
                "--checkpoint.dir", "file://" + tempDir.toAbsolutePath()
        });

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        AdherenceJob.buildTopology(env, jobConfig);
        return env.getStreamGraph();
    }

    /**
     * CLAUDE.md §4: operator UIDs on every operator, for savepoint compatibility. An operator
     * without an explicit UID gets a hash derived from the graph structure, so its identity
     * changes silently whenever the surrounding topology changes.
     */
    @Test
    void everyOperatorCarriesAUid() throws Exception {
        assertThat(buildGraph().getStreamNodes())
                .allSatisfy(node -> assertThat(node.getTransformationUID())
                        .as("operator '%s' has no UID (CLAUDE.md §4)", node.getOperatorName())
                        .isNotNull());
    }

    @Test
    void operatorUidsAreAssignedAndFrozen() throws Exception {
        Set<String> uids = buildGraph().getStreamNodes().stream()
                .map(StreamNode::getTransformationUID)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        assertThat(uids).contains(
                "chronic-class-filter",
                "adherence-process",
                "gap-risk-alerts-sink",
                "lapsed-alerts-sink",
                "pdc-snapshots-sink",
                "dead-letter-sink",
                "ndc-drug-class-ref-watermarks",
                "alert-lead-time-ref-watermarks",
                "rx-fill-events-dead-letter-record",
                "ndc-drug-class-ref-dead-letter-record",
                "alert-lead-time-ref-dead-letter-record");
    }

    @Test
    void broadcastStateDescriptorNamesAreFrozen() {
        assertThat(ChronicClassFilterFunction.NDC_CLASS_DESCRIPTOR.getName())
                .isEqualTo("ndc-class-state");
        assertThat(AdherenceProcessFunction.LEAD_TIME_DESCRIPTOR.getName())
                .isEqualTo("lead-time-state");
    }
}
