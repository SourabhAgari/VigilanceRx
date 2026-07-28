package com.healthcare.rxvigilance.serialization.encode;

import com.healthcare.rxvigilance.serialization.codec.AvroRecordEncoder;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;

public class AvroKeyValueSerializer<T> {

    private final KafkaAvroSerializer avroSerializer;
    private final AvroRecordEncoder<T> serializer;
    private final String topic;

    public AvroKeyValueSerializer(SchemaRegistryClient schemaRegistryClient,
                                  AvroRecordEncoder<T> avroSerializer,
                                  String topic) {
        this.avroSerializer = new KafkaAvroSerializer(schemaRegistryClient);
        this.serializer = avroSerializer;
        this.topic = topic;
    }

    public byte[] serialize(T value) {
        GenericRecord genericRecord = serializer.encode(value);
        return avroSerializer.serialize(topic, genericRecord);
    }
}
