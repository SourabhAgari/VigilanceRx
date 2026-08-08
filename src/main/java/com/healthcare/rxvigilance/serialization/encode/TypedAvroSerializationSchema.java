package com.healthcare.rxvigilance.serialization.encode;

import com.healthcare.rxvigilance.serialization.codec.AvroRecordEncoder;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.util.Map;

public class TypedAvroSerializationSchema <T> implements SerializationSchema<T> {

    private static final int SCHEMA_CACHE_CAPACITY = 1000;
    private final String schemaRegistryUrl;
    private final AvroRecordEncoder<T> serializer;
    private final String topic;
    private transient AvroKeyValueSerializer<T> avroRecordSerializer;
    private final Map<String, String> registryConfig;

    public TypedAvroSerializationSchema(String schemaRegistryUrl,
                                        Map<String,String> registryConfig,
                                        AvroRecordEncoder<T> serializer, String topic) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.registryConfig = registryConfig;
        this.serializer = serializer;
        this.topic = topic;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        SchemaRegistryClient client = new CachedSchemaRegistryClient(schemaRegistryUrl, SCHEMA_CACHE_CAPACITY, registryConfig);
        this.avroRecordSerializer = new AvroKeyValueSerializer<>(client, serializer, topic,schemaRegistryUrl);
    }

    @Override
    public byte[] serialize(T element) {
        return avroRecordSerializer.serialize(element);
    }
}
