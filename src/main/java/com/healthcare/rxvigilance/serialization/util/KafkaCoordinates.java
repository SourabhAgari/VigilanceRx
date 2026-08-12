package com.healthcare.rxvigilance.serialization.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public record KafkaCoordinates(String topic, int partition, long offset, long timestamp) {
    public static KafkaCoordinates from(ConsumerRecord<?, ?> consumerRecord) {
        return new KafkaCoordinates(
                consumerRecord.topic(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                consumerRecord.timestamp());
    }
}

