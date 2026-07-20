package com.healthcare.rxvigilance;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SmokeJob {
    public static void main(String[] args) throws Exception{
        ParameterTool params = ParameterTool.fromArgs(args);

        String brokers = params.getRequired("kafka.brokers");
        String registryUrl = params.getRequired("schema.registry.url");
        String checkpointDir = params.getRequired("checkpoint.dir");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(30_000);
        env.getCheckpointConfig().setCheckpointStorage(checkpointDir);

        env.execute("smoke-job");
    }
}
