package com.healthcare.rxvigilance.serde;

import com.healthcare.rxvigilance.serde.mapper.AvroValueSerializer;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;

public class AvroRecordSerializer <T> {

    private final KafkaAvroSerializer avroSerializer;
    private final AvroValueSerializer<T> serializer;
    private final String topic;

    public AvroRecordSerializer(SchemaRegistryClient schemaRegistryClient,
                                AvroValueSerializer<T> avroSerializer,
                                String topic) {
        this.avroSerializer = new KafkaAvroSerializer(schemaRegistryClient);
        this.serializer = avroSerializer;
        this.topic = topic;
    }

    public byte[] serialize(T value) {
        GenericRecord genericRecord = serializer.serialize(value);
        return avroSerializer.serialize(topic, genericRecord);
    }
}
