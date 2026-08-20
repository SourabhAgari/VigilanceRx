package com.healthcare.rxvigilance.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Map;

/**
 * Configuration for Kafka and Schema Registry connectivity.
 *
 * <p>Encapsulates connection settings, validates required endpoints, and
 * provides Schema Registry authentication configuration when SASL credentials
 * are available.
 */
public record KafkaConnectionConfig(
        String brokers,
        String schemaRegistryUrl,
        String saslUserName,
        String saslPassword,
        String securityProtocol,
        String saslMechanism
) {

    /**
     * Validates the required Kafka and Schema Registry connection endpoints.
     *
     * <p>This compact constructor is a canonical record constructor introduced
     * with Java 16 and performs validation before the record is created.
     *
     * @param brokers Kafka bootstrap server addresses
     * @param schemaRegistryUrl Schema Registry endpoint
     * @param saslUserName SASL username, if authentication is configured
     * @param saslPassword SASL password, if authentication is configured
     * @param securityProtocol Kafka security protocol
     * @param saslMechanism Kafka SASL mechanism
     * @throws IllegalArgumentException if the Kafka brokers or Schema Registry
     *                                  URL is null or blank
     */
    public KafkaConnectionConfig {
        if (brokers == null || brokers.isBlank()) {
            throw new IllegalArgumentException("brokers cannot be null or blank");
        }
        if (schemaRegistryUrl == null || schemaRegistryUrl.isBlank()) {
            throw new IllegalArgumentException("schemaRegistryUrl cannot be null or blank");
        }
    }

    /**
     * Determines whether both SASL credentials are available.
     *
     * @return {@code true} when both the SASL username and password are present;
     *         {@code false} otherwise
     */
    public boolean hasSaslCredentials() {
        return saslUserName != null && saslPassword != null;
    }

    /**
     * Builds the Schema Registry client configuration required for
     * authenticated connections.
     *
     * <p>Returns an empty configuration when SASL credentials are unavailable,
     * allowing the same configuration model to support unauthenticated local
     * Schema Registry environments.
     *
     * @return Schema Registry client properties containing authentication
     *         settings when SASL credentials are available
     */
    public Map<String, String> registryConfig() {
        if (!hasSaslCredentials()) {
            return Map.of();
        }
        return Map.of(
                "basic.auth.credentials.source", "USER_INFO",
                "schema.registry.basic.auth.user.info", saslUserName + ":" + saslPassword);
    }

    /**
     * Creates a {@code KafkaConnectionConfig} from the supplied Flink
     * parameters and environment-provided SASL credentials.
     *
     * @param params Flink parameters containing Kafka and Schema Registry settings
     * @return typed Kafka and Schema Registry connection configuration
     */
    public static KafkaConnectionConfig fromParams(ParameterTool params) {
        return new KafkaConnectionConfig(
                params.getRequired("kafka.brokers"),
                params.getRequired("schema.registry.url"),
                System.getenv("KAFKA_SASL_USERNAME"),
                System.getenv("KAFKA_SASL_PASSWORD"),
                params.get("kafka.security.protocol", null),
                params.get("kafka.sasl.mechanism", null));
    }

}