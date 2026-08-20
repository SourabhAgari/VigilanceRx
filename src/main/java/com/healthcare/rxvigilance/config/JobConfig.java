package com.healthcare.rxvigilance.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Central configuration for the Flink job.
 *
 * <p>Loads configuration from the selected classpath profile, an optional
 * external configuration file, and command-line arguments. Configuration
 * sources are merged with command-line arguments taking the highest precedence.
 *
 * <p>The merged parameters are converted into typed configuration objects for
 * Kafka, checkpointing, state management, and watermarking.
 */
public final class JobConfig {

    private final ParameterTool parameters;

    private final KafkaConnectionConfig kafkaConfig;
    private final CheckpointConfig checkpointConfig;
    private final StateBackEndConfig stateBackEndConfig;
    private final WatermarkConfig watermarkConfig;

    /**
     * Creates a job configuration from the supplied typed configuration objects
     * and the final merged parameter set.
     *
     * @param kafkaConfig Kafka and Schema Registry connection configuration
     * @param checkpointConfig Flink checkpoint configuration
     * @param stateBackEndConfig Flink state backend and state TTL configuration
     * @param watermarkConfig Flink watermark configuration
     * @param parameters final merged Flink parameters
     */
    private JobConfig(KafkaConnectionConfig kafkaConfig,
                      CheckpointConfig checkpointConfig,
                      StateBackEndConfig stateBackEndConfig,
                      WatermarkConfig watermarkConfig,
                      ParameterTool parameters) {
        this.kafkaConfig = kafkaConfig;
        this.checkpointConfig = checkpointConfig;
        this.stateBackEndConfig = stateBackEndConfig;
        this.watermarkConfig = watermarkConfig;
        this.parameters = parameters;
    }

    /**
     * Builds the complete job configuration from the supplied command-line arguments.
     *
     * <p>Configuration is resolved using the following precedence:
     * classpath profile, optional external configuration file, and command-line
     * arguments, with later sources overriding earlier values.
     *
     * @param args command-line arguments passed to the Flink job
     * @return fully constructed job configuration
     * @throws IOException if the external configuration file or classpath profile
     *                     cannot be read
     */
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
                CheckpointConfig.fromParams(merged),
                StateBackEndConfig.fromParams(merged),
                WatermarkConfig.fromParams(merged), merged);
    }

    /**
     * Loads the configuration profile from the application classpath.
     *
     * <p>The profile is resolved using the {@code application-{profile}.properties}
     * naming convention. Property values are trimmed and converted into a
     * {@link ParameterTool} for subsequent configuration merging.
     *
     * @param profile configuration profile name
     * @return parameters loaded from the selected classpath profile
     * @throws IOException if the profile properties cannot be read
     */
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

    /**
     * Returns the Kafka and Schema Registry connection configuration.
     *
     * @return Kafka connection configuration
     */
    public KafkaConnectionConfig getKafkaConfig() {
        return kafkaConfig;
    }

    /**
     * Returns the Flink checkpoint configuration.
     *
     * @return checkpoint configuration
     */
    public CheckpointConfig getCheckpointConfig() {
        return checkpointConfig;
    }

    /**
     * Returns the Flink state backend and state TTL configuration.
     *
     * @return state backend configuration
     */
    public StateBackEndConfig getStateBackEndConfig() {
        return stateBackEndConfig;
    }

    /**
     * Returns the Flink watermark configuration.
     *
     * @return watermark configuration
     */
    public WatermarkConfig getWatermarkConfig() {
        return watermarkConfig;
    }

    /**
     * Returns the final merged Flink parameters.
     *
     * @return merged job parameters
     */
    public ParameterTool getParams() {
        return parameters;
    }
}