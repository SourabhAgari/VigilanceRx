package com.healthcare.rxvigilance.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link JobConfig}.
 *
 * <p>Verifies configuration loading from the classpath profile and external
 * configuration file, command-line argument precedence, default profile
 * behavior, and handling of unknown profiles.
 */
class JobConfigTest {

    /**
     * Verifies that command-line arguments override values supplied through
     * an external configuration file while other file values remain available.
     *
     * @param tempDir temporary directory used to create the external configuration file
     * @throws IOException if the temporary configuration file cannot be written
     */
    @Test
    void cliArgOverridesConfigFileValue(@TempDir Path tempDir) throws IOException {
        Path configFile = tempDir.resolve("test.properties");
        Properties fileProperties = new Properties();
        fileProperties.setProperty("kafka.brokers", "file-value:9092");
        fileProperties.setProperty("schema.registry.url", "http://file-registry:8081");
        fileProperties.setProperty("checkpoint.dir", "file:'''tmp/file-checkpoints");

        try (var out = Files.newOutputStream(configFile)) {
            fileProperties.store(out, "null");
        }

        JobConfig config = JobConfig.fromArgs(new String[]{
                "--config.file", configFile.toString(),
                "--kafka.brokers", "cli-value:9092",
        });

        assertThat(config.getKafkaConfig().brokers()).isEqualTo("cli-value:9092");
        assertThat(config.getKafkaConfig().schemaRegistryUrl())
                .isEqualTo("http://file-registry:8081");
    }

    /**
     * Verifies that the default local profile is loaded from the classpath
     * when no profile or external configuration is specified.
     *
     * @throws IOException if the classpath configuration cannot be loaded
     */
    @Test
    void defaultLocalProfileLoadsFromClasspath() throws IOException {
        JobConfig config = JobConfig.fromArgs(new String[]{});

        assertThat(config.getKafkaConfig().brokers()).isEqualTo("localhost:9092");
        assertThat(config.getKafkaConfig().schemaRegistryUrl())
                .isEqualTo("http://localhost:8081");
        assertThat(config.getCheckpointConfig().checkpointDirectory())
                .isEqualTo("file:///tmp/rx-vigilance-checkpoints");
        assertThat(config.getStateBackEndConfig().ttlDays()).isEqualTo(400);
        assertThat(config.getWatermarkConfig().outOfOrderness())
                .isEqualTo(Duration.ofHours(24));
        assertThat(config.getWatermarkConfig().idleness())
                .isEqualTo(Duration.ofMinutes(5));
    }

    /**
     * Verifies that an unknown profile does not prevent configuration loading
     * when all required configuration values are supplied through CLI arguments.
     *
     * @throws IOException if configuration loading fails
     */
    @Test
    void unknownProfileFallsBackToEmptyWithoutError() throws IOException {
        JobConfig config = JobConfig.fromArgs(new String[]{
                "--profile", "nonexistent",
                "--kafka.brokers", "cli-value:9092",
                "--schema.registry.url", "http://cli-registry:8081",
                "--checkpoint.dir", "file:///tmp/cli-checkpoints"
        });

        assertThat(config.getKafkaConfig().brokers()).isEqualTo("cli-value:9092");
    }
}