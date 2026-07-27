package com.healthcare.rxvigilance.serialization.util;

import com.healthcare.rxvigilance.config.KafkaConnectionConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.Properties;

public class KafkaSourceUtil {
    private KafkaSourceUtil() {}

    public static OffsetsInitializer startingOffsetsInitializer(ParameterTool parameters) {
        String policy = parameters.get("kafka.starting.offsets", "earliest");
        return switch (policy) {
            case "earliest" -> OffsetsInitializer.earliest();
            case "latest" -> OffsetsInitializer.latest();
            default -> throw new IllegalArgumentException( "kafka.starting.offsets " +
                    "must be 'earliest' or 'latest', got: " + policy);
        };
    }

    public static Properties securityProperties(KafkaConnectionConfig kafkaConnectionConfig) {
        Properties properties = new Properties();
        if(kafkaConnectionConfig.hasSaslCredentials()) {
            properties.setProperty("security.protocol", kafkaConnectionConfig.securityProtocol());
            properties.setProperty("sasl.mechanism", kafkaConnectionConfig.saslMechanism());
            properties.setProperty("sasl.jaas.config",
                    "org.apache.kafka.common.security.scram.ScramLoginModule required username=\""
                            + kafkaConnectionConfig.saslUserName() + "\" password=\""
                            + kafkaConnectionConfig.saslPassword() + "\";");
        }
        return properties;
    }
}
