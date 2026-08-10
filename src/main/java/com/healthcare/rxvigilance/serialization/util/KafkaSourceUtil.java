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

    public static Properties producerProperties(
            KafkaConnectionConfig kafkaConnectionConfig,
            ParameterTool parameters) {

        Properties properties = securityProperties(kafkaConnectionConfig);

        // Exactly-once sinks hold a transaction open from one checkpoint to the
        // next and commit on checkpoint completion. If the job is down longer
        // than this, the broker discards the open transaction and the alerts in
        // it are lost - so the value must exceed the worst restart, not the
        // checkpoint interval.
        //
        // 900000 (15 min) is the broker's maximum, confirmed by probing this
        // cluster (#133). Flink's own default is 1 hour and was rejected;
        // production Flink deployments usually raise the broker's
        // transaction.max.timeout.ms instead, which serverless does not allow.
        // We asked for an hour and took the cap. Override per environment with
        // kafka.transaction.timeout.ms.
        properties.setProperty(
                "transaction.timeout.ms",
                parameters.get("kafka.transaction.timeout.ms", "900000"));

        return properties;
    }
}
