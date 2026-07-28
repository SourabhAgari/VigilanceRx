package com.healthcare.rxvigilance.serialization.util;

import com.healthcare.rxvigilance.config.KafkaConnectionConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaSourceUtilTest {

    @Test
    void startingOffsetsInitializerDefaultsToEarliest() {
        OffsetsInitializer offsets = KafkaSourceUtil.startingOffsetsInitializer(ParameterTool.fromMap(Map.of()));

        assertThat(offsets).isInstanceOf(OffsetsInitializer.earliest().getClass());
    }

    @Test
    void startingOffsetsInitializerReadsLatest() {
        ParameterTool params = ParameterTool.fromMap(Map.of("kafka.starting.offsets", "latest"));

        assertThat(KafkaSourceUtil.startingOffsetsInitializer(params)).isInstanceOf(OffsetsInitializer.latest().getClass());
    }

    @Test
    void startingOffsetsInitializerRejectsInvalidPolicy() {
        ParameterTool params = ParameterTool.fromMap(Map.of("kafka.starting.offsets", "bogus"));

        assertThatThrownBy(() -> KafkaSourceUtil.startingOffsetsInitializer(params))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("kafka.starting.offsets");
    }

    @Test
    void securityPropertiesEmptyWhenNoSaslCredentials() {
        KafkaConnectionConfig kafkaConfig = new KafkaConnectionConfig(
                "localhost:9092", "http://localhost:8081",
                null, null, null, null);

        assertThat(KafkaSourceUtil.securityProperties(kafkaConfig)).isEmpty();
    }

    @Test
    void securityPropertiesSetWhenSaslCredentialsPresent() {
        KafkaConnectionConfig kafkaConfig = new KafkaConnectionConfig(
                "broker:9092", "http://registry", "user", "pass", "SASL_SSL", "SCRAM-SHA-256");

        Properties properties = KafkaSourceUtil.securityProperties(kafkaConfig);

        assertThat(properties.getProperty("security.protocol")).isEqualTo("SASL_SSL");
        assertThat(properties.getProperty("sasl.mechanism")).isEqualTo("SCRAM-SHA-256");
        assertThat(properties.getProperty("sasl.jaas.config"))
                .contains("username=\"user\"")
                .contains("password=\"pass\"");
    }

}
