package com.healthcare.rxvigilance.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaConnectionConfigTest {

    @Test
    void rejectBlankBrokers() {
        assertThatThrownBy(() -> new KafkaConnectionConfig(
                "", "http://registry", null, null, null, null
        )).isInstanceOf(IllegalArgumentException.class);
    }

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

    @Test
    void hasSaslCredentialsReflectsPresence() {
        KafkaConnectionConfig withCredentials = new KafkaConnectionConfig(
                "localhost:9092",
                "http://registry",
                "user",
                "pass", null, null);

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

    @Test
    void registryConfigCarriesSaslCredentialsWhenPresent() {
        KafkaConnectionConfig config = new KafkaConnectionConfig(
                "localhost:9092", "http://localhost:8081",
                "user", "pass", "SASL_SSL", "SCRAM-SHA-256");

        assertThat(config.registryConfig())
                .containsEntry("basic.auth.credentials.source", "USER_INFO")
                .containsEntry("schema.registry.basic.auth.user.info", "user:pass");
    }

    @Test
    void registryConfigIsEmptyWithoutCredentials() {
        KafkaConnectionConfig config = new KafkaConnectionConfig(
                "localhost:9092", "http://localhost:8081",
                null, null, null, null);

        assertThat(config.registryConfig()).isEmpty();
    }


}
