package com.healthcare.rxvigilance.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaConnectionConfigTest {

    @Test
    void rejectBlankBrokers() {
        assertThatThrownBy(() -> new KafkaConnectionConfig(
                "", "http://registry", null, null
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsBlankSchemaRegistryUrl() {
        assertThatThrownBy(() -> new KafkaConnectionConfig(
                "localhost:9092", "", null, null
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void hasSaslCredentialsReflectsPresence() {
        KafkaConnectionConfig withCredentials = new KafkaConnectionConfig(
                "localhost:9092",
                "http://registry",
                "user",
                "pass");

        KafkaConnectionConfig withoutCredentials = new KafkaConnectionConfig(
                "localhost:9092",
                "http://registry",
                null,
                null
        );
        assertThat(withCredentials.hasSaslCredentials()).isTrue();
        assertThat(withoutCredentials.hasSaslCredentials()).isFalse();
    }
}
