package com.healthcare.rxvigilance.serialization;

import com.healthcare.rxvigilance.serialization.mapper.AvroValueSerializer;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class TypedAvroSerializationSchema <T> implements SerializationSchema<T> {

    private static final int SCHEMA_CACHE_CAPACITY = 1000;
    private final String schemaRegistryUrl;
    private final AvroValueSerializer<T> serializer;
    private final String topic;
    private transient AvroRecordSerializer<T> avroRecordSerializer;

    public TypedAvroSerializationSchema(String schemaRegistryUrl, AvroValueSerializer<T> serializer, String topic) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.serializer = serializer;
        this.topic = topic;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        SchemaRegistryClient client = new CachedSchemaRegistryClient(schemaRegistryUrl, SCHEMA_CACHE_CAPACITY);
        this.avroRecordSerializer = new AvroRecordSerializer<>(client, serializer, topic);
    }

    @Override
    public byte[] serialize(T element) {
        return avroRecordSerializer.serialize(element);
    }
}
