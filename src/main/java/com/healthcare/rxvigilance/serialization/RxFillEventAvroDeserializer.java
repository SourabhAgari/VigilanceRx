package com.healthcare.rxvigilance.serialization;

import com.healthcare.rxvigilance.domain.Channel;
import com.healthcare.rxvigilance.domain.EventType;
import com.healthcare.rxvigilance.domain.RxFillEvent;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;

public class RxFillEventAvroDeserializer {
    private static final Conversions.DecimalConversion DECIMAL_CONVERSION = new Conversions.DecimalConversion();
    private final KafkaAvroDeserializer kafkaAvroDeserializer;

    public RxFillEventAvroDeserializer(SchemaRegistryClient schemaRegistryClient) {
        this.kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
    }

    public DeserializationResult deserialize(byte[] bytes) {
        try {
            GenericRecord genericRecord = (GenericRecord) kafkaAvroDeserializer.deserialize(null, bytes);
            return DeserializationResult.success(toRxFillEvent(genericRecord));
        } catch (SerializationException | ClassCastException | IllegalArgumentException | NullPointerException e) {
            return DeserializationResult.failure(bytes, e.getMessage());
        }
    }

    private RxFillEvent toRxFillEvent(GenericRecord genericRecord) {
        Schema quantitySchema = genericRecord.getSchema().getField("quantity").schema();
        ByteBuffer quantityBytes = (ByteBuffer) genericRecord.get("quantity");
        BigDecimal quantity = DECIMAL_CONVERSION.fromBytes(
                quantityBytes, quantitySchema, LogicalTypes.decimal(10, 2)
        );
        Object originalClaimIdRaw = genericRecord.get("originalClaimId");
        return new RxFillEvent(
                EventType.valueOf(genericRecord.get("eventType").toString()),
                genericRecord.get("claimId").toString(),
                genericRecord.get("memberId").toString(),
                genericRecord.get("ndcCode").toString(),
                LocalDate.ofEpochDay((Integer) genericRecord.get("fillDate")),
                (Integer) genericRecord.get("daySupply"),
                quantity,
                genericRecord.get("pharmacyId").toString(),
                genericRecord.get("rxNumber").toString(),
                (Integer) genericRecord.get("refillsAuthorized"),
                Channel.valueOf(genericRecord.get("dispensingChannel").toString()),
                originalClaimIdRaw == null ? null : originalClaimIdRaw.toString());
    }
}
