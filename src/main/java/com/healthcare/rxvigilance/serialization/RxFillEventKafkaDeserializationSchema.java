package com.healthcare.rxvigilance.serialization;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class RxFillEventKafkaDeserializationSchema implements KafkaRecordDeserializationSchema<DeserializationResult> {
    private static final int SCHEMA_CACHE_CAPACITY = 1000;
    private final String schemaRegistryUrl;

    private transient RxFillEventAvroDeserializer deserializer;

    public RxFillEventKafkaDeserializationSchema(String schemaRegistryUrl) {
        this.schemaRegistryUrl =  schemaRegistryUrl;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl,SCHEMA_CACHE_CAPACITY);
        this.deserializer = new RxFillEventAvroDeserializer(schemaRegistryClient);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord,
                            Collector<DeserializationResult> collector) throws IOException {
        collector.collect(deserializer.deserialize(consumerRecord.value()));
    }

    @Override
    public TypeInformation<DeserializationResult> getProducedType() {
        return TypeInformation.of(DeserializationResult.class);
    }
}
