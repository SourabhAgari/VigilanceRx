package com.healthcare.rxvigilance.serialization.decode;

import com.healthcare.rxvigilance.serialization.util.KafkaCoordinates;
import com.healthcare.rxvigilance.serialization.util.KafkaSourceResult;
import com.healthcare.rxvigilance.serialization.codec.AvroRecordDecoder;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.Map;

public final class TypedAvroDeserialisationSchema<T>
        implements KafkaRecordDeserializationSchema<KafkaSourceResult<T>> {
    private static final int SCHEMA_CACHE_CAPACITY = 10000;
    private final String schemaRegistryUrl;
    private final AvroRecordDecoder<T> mapper;
    private final TypeInformation<KafkaSourceResult<T>> producedType;
    private transient AvroKeyValueDeSerializer<T> deserializer;
    private final Map<String,String> registryConfig;

    public TypedAvroDeserialisationSchema(String schemaRegistryUrl,
                                          AvroRecordDecoder<T> mapper,
                                          Map<String,String> registryConfig,
                                          TypeInformation<KafkaSourceResult<T>> producedType) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.mapper = mapper;
        this.producedType = producedType;
        this.registryConfig = registryConfig;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        SchemaRegistryClient client = new CachedSchemaRegistryClient(schemaRegistryUrl, SCHEMA_CACHE_CAPACITY,registryConfig);
        this.deserializer = new AvroKeyValueDeSerializer<>(client, mapper);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<KafkaSourceResult<T>> collector) throws IOException {
        String key = consumerRecord.key() == null ? null : new String(consumerRecord.key());
        // The last point where the broker position is available. Attached to every
        // result, not just failures: #148's per-record DEBUG logs want it too. #149.
        collector.collect(deserializer.deserialize(key, consumerRecord.value())
                .withCoordinates(KafkaCoordinates.from(consumerRecord)));
    }

    @Override
    public TypeInformation<KafkaSourceResult<T>> getProducedType() {
        return producedType;
    }
}
