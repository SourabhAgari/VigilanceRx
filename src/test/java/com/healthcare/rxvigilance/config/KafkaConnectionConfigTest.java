package com.healthcare.rxvigilance.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link KafkaConnectionConfig}.
 *
 * <p>Verifies validation of required Kafka and Schema Registry connection
 * settings, SASL credential detection, and generation of Schema Registry
 * authentication configuration.
 */
class KafkaConnectionConfigTest {

    /**
     * Verifies that blank Kafka broker configuration is rejected.
     */
    @Test
    void rejectBlankBrokers() {
        assertThatThrownBy(() -> new KafkaConnectionConfig(
                "", "http://registry", null, null, null, null
        )).isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Verifies that a blank Schema Registry URL is rejected.
     */
    @Test
    void rejectsBlankSchemaRegistryUrl() {
        assertThatThrownBy(() -> new KafkaConnectionConfig(
                "localhost:9092",
                "",
                null,
                null,
                null,
                null
        )).isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Verifies that SASL credential availability is determined by the presence
     * of both username and password.
     */
    @Test
    void hasSaslCredentialsReflectsPresence() {
        KafkaConnectionConfig withCredentials = new KafkaConnectionConfig(
                "localhost:9092",
                "http://registry",
                "user",
                "pass",
                null,
                null);

        KafkaConnectionConfig withoutCredentials = new KafkaConnectionConfig(
                "localhost:9092",
                "http://registry",
                null,
                null,
                null,
                null
        );

        assertThat(withCredentials.hasSaslCredentials()).isTrue();
        assertThat(withoutCredentials.hasSaslCredentials()).isFalse();
    }

    /**
     * Verifies that Schema Registry authentication properties contain the
     * configured SASL credentials when credentials are available.
     */
    @Test
    void registryConfigCarriesSaslCredentialsWhenPresent() {
        KafkaConnectionConfig config = new KafkaConnectionConfig(
                "localhost:9092",
                "http://localhost:8081",
                "user",
                "pass",
                "SASL_SSL",
                "SCRAM-SHA-256");

        assertThat(config.registryConfig())
                .containsEntry("basic.auth.credentials.source", "USER_INFO")
                .containsEntry("schema.registry.basic.auth.user.info", "user:pass");
    }

    /**
     * Verifies that no Schema Registry authentication properties are returned
     * when SASL credentials are unavailable.
     */
    @Test
    void registryConfigIsEmptyWithoutCredentials() {
        KafkaConnectionConfig config = new KafkaConnectionConfig(
                "localhost:9092",
                "http://localhost:8081",
                null,
                null,
                null,
                null);

        assertThat(config.registryConfig()).isEmpty();
    }
}