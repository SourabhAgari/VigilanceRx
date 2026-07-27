package com.healthcare.rxvigilance.serialization;

import com.healthcare.rxvigilance.serialization.mapper.AvroValueMapper;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;

public class AvroKeyValueDeSerializer<T> {
    private final KafkaAvroDeserializer deserializer;
    private final AvroValueMapper<T> mapper;

    public AvroKeyValueDeSerializer(SchemaRegistryClient client, AvroValueMapper<T> mapper) {
        this.deserializer = new KafkaAvroDeserializer(client);
        this.mapper = mapper;
    }

    public KafkaSourceResult<T> deserialize(String key, byte[] valueBytes){
        try {
            GenericRecord genericRecord = (GenericRecord) deserializer.deserialize(null, valueBytes);
            return KafkaSourceResult.success(mapper.map(key, genericRecord));
        }  catch (SerializationException | ClassCastException | IllegalArgumentException | NullPointerException e) {
            return KafkaSourceResult.failure(valueBytes, e.getMessage());
        }
    }

}
