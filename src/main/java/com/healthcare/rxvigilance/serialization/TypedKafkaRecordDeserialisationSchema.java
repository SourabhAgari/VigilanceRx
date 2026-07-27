package com.healthcare.rxvigilance.serialization;

import com.healthcare.rxvigilance.serialization.mapper.AvroValueMapper;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public final class TypedKafkaRecordDeserialisationSchema<T>
        implements KafkaRecordDeserializationSchema<KafkaSourceResult<T>> {
    private static final int SCHEMA_CACHE_CAPACITY = 10000;
    private final String schemaRegistryUrl;
    private final AvroValueMapper<T> mapper;
    private final TypeInformation<KafkaSourceResult<T>> producedType;
    private transient AvroKeyValueDeSerializer<T> deserializer;

    public TypedKafkaRecordDeserialisationSchema(String schemaRegistryUrl,
                                                 AvroValueMapper<T> mapper,
                                                 TypeInformation<KafkaSourceResult<T>> producedType) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.mapper = mapper;
        this.producedType = producedType;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        SchemaRegistryClient client = new CachedSchemaRegistryClient(schemaRegistryUrl, SCHEMA_CACHE_CAPACITY);
        this.deserializer = new AvroKeyValueDeSerializer<>(client, mapper);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<KafkaSourceResult<T>> collector) throws IOException {
        String key = consumerRecord.key() == null ? null : new String(consumerRecord.key());
        collector.collect(deserializer.deserialize(key, consumerRecord.value()));
    }

    @Override
    public TypeInformation<KafkaSourceResult<T>> getProducedType() {
        return producedType;
    }
}
