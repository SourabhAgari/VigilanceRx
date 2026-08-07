package com.healthcare.rxvigilance.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Map;

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

    /**
     * Schema registry client config. Redpanda Cloud's registry requires the same
     * SASL credentials the Kafka client uses; a local registry needs none, which is
     * why this gap survived every local run and every integration test (#109).
     *
     * Returns a Map rather than exposing this record, because the serialization
     * schemas that consume it are Serializable and shipped to TaskManagers.
     */
    public Map<String, String> registryConfig() {
        if (!hasSaslCredentials()) {
            return Map.of();
        }
        return Map.of(
                "basic.auth.credentials.source", "USER_INFO",
                "schema.registry.basic.auth.user.info", saslUserName + ":" + saslPassword);
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