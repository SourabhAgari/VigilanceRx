package com.healthcare.rxvigilance.config;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;

class JobConfigTest {

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
        assertThat(config.getKafkaConfig().schemaRegistryUrl()).isEqualTo("http://file-registry:8081");
    }

    @Test
    void defaultLocalProfileLoadsFromClasspath() throws IOException {
        JobConfig config = JobConfig.fromArgs(new String[]{});
        assertThat(config.getKafkaConfig().brokers()).isEqualTo("localhost:9092");
        assertThat(config.getKafkaConfig().schemaRegistryUrl()).isEqualTo("http://localhost:8081");
        assertThat(config.getCheckpointConfig().checkpointDirectory()).isEqualTo("file:///tmp/rx-vigilance-checkpoints");
    }

}
