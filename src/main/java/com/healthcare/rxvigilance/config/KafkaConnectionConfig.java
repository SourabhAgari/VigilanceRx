package com.healthcare.rxvigilance.config;

import org.apache.flink.api.java.utils.ParameterTool;

public record KafkaConnectionConfig(
        String brokers,
        String schemaRegistryUrl,
        String saslUserName,
        String saslPassword,
        String securityProtocol,
        String saslMechanism
) {

    public KafkaConnectionConfig {
        if (brokers == null || brokers.isBlank()) {
            throw new IllegalArgumentException("brokers cannot be null or blank");
        }
        if (schemaRegistryUrl == null || schemaRegistryUrl.isBlank()) {
            throw new IllegalArgumentException("schemaRegistryUrl cannot be null or blank");
        }
    }

    public boolean hasSaslCredentials() {
        return saslUserName != null && saslPassword != null;
    }

    public static KafkaConnectionConfig fromParams(ParameterTool params) {
        return new KafkaConnectionConfig(
                params.getRequired("kafka.brokers"),
                params.getRequired("schema.registry.url"),
                System.getenv("KAFKA_SASL_USERNAME"),
                System.getenv("KAFKA_SASL_PASSWORD"),
                params.get("kafka.security.protocol",null),
                params.get("kafka.sasl.mechanism",null));
    }

}