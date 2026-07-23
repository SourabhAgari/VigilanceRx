package com.healthcare.rxvigilance.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public final class JobConfig {

    private final KafkaConnectionConfig kafkaConfig;
    private final CheckpointConfig checkpointConfig;

    private JobConfig(KafkaConnectionConfig kafkaConfig, CheckpointConfig checkpointConfig) {
        this.kafkaConfig = kafkaConfig;
        this.checkpointConfig = checkpointConfig;
    }

    public static JobConfig fromArgs(String[] args) throws IOException {
        ParameterTool cliParams = ParameterTool.fromArgs(args);

        String profile = cliParams.get("profile", "local");
        ParameterTool profileParams = loadClassPathProfile(profile);

        ParameterTool merged = profileParams;
        if (cliParams.has("config.file")) {
            ParameterTool mountedParams = ParameterTool.fromPropertiesFile(cliParams.get("config.file"));
            merged = profileParams.mergeWith(mountedParams);
        }
        merged = merged.mergeWith(cliParams);

        return new JobConfig(KafkaConnectionConfig.fromParams(merged),
                CheckpointConfig.fromParams(merged));
    }

    private static ParameterTool loadClassPathProfile(String profile) throws IOException {
        String propsFile = "application-" + profile + ".properties";
        Properties props = new Properties();
        try (InputStream is = JobConfig.class.getClassLoader().getResourceAsStream(propsFile)) {
            if (is != null) {
                props.load(is);
            }
        }
        Map<String, String> asMap = props.stringPropertyNames().stream()
                .collect(Collectors.toMap(k -> k, k -> props.getProperty(k).trim()));
        return ParameterTool.fromMap(asMap);
    }

    public KafkaConnectionConfig getKafkaConfig() {
        return kafkaConfig;
    }

    public CheckpointConfig getCheckpointConfig() {
        return checkpointConfig;
    }
}
