package com.healthcare.rxvigilance.serialization;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.Month;

class RxFillEventDeserializerTest {

    private Schema rxFillEventSchema;
    private KafkaAvroSerializer serializer;
    private RxFillEventAvroDeserializer deserializer;

    @BeforeEach
    void setUp() throws Exception {
        try (InputStream avsc = getClass().getResourceAsStream("/rx-fill-event.avsc")) {
            rxFillEventSchema = new Schema.Parser().parse(avsc);
        }

        SchemaRegistryClient registryClient = new MockSchemaRegistryClient();
        registryClient.register("rx-fill-events-value", rxFillEventSchema);

        serializer = new KafkaAvroSerializer(registryClient);
        deserializer = new RxFillEventAvroDeserializer(registryClient);
    }

    @Test
    void roundTripsAValidFillEvent() {
        GenericRecord genericRecord = buildFillRecord("CLM-1",
                LocalDate.of(2026, Month.JANUARY, 1),
                30,
                new BigDecimal("30.00"),
                "FILL",null);
        byte[] bytes = serializer.serialize("rx-fill-events", genericRecord);
        DeserializationResult deserializationResult = deserializer.deserialize(bytes);

        assertThat(deserializationResult.isSuccess()).isTrue();
        assertThat(deserializationResult.event().claimId()).isEqualTo("CLM-1");
        assertThat(deserializationResult.event().fillDate()).isEqualTo(LocalDate.of(2026, Month.JANUARY, 1));
        assertThat(deserializationResult.event().daySupply()).isEqualTo(30);
        assertThat(deserializationResult.event().quantity()).isEqualByComparingTo("30.00");
        assertThat(deserializationResult.event().originalClaimId()).isNull();
    }

    @Test
    void malformedBytesGoToDeadLetterInsteadOfThrowing() {
        byte[] badMagicByte = {0x05,0x00,0x00,0x00,0x01};
        DeserializationResult deserializationResult = deserializer
                .deserialize(badMagicByte);

        assertThat(deserializationResult.isSuccess()).isFalse();
        assertThat(deserializationResult.rawBytes()).isEqualTo(badMagicByte);
        assertThat(deserializationResult.errorMessage()).isNotNull();
    }

    @Test
    void roundTripsAReversalWithOriginalClaimId() {
        GenericRecord record = buildFillRecord(
                "CLM-2", LocalDate.of(2026,
                        Month.JANUARY, 1),
                30, new BigDecimal("30.00"),
                "REVERSAL",
                "CLM-1");

        byte[] bytes = serializer.serialize("rx-fill-events", record);
        DeserializationResult deserializationResult = deserializer.deserialize(bytes);

        assertThat(deserializationResult.isSuccess()).isTrue();
        assertThat(deserializationResult.event().claimId()).isEqualTo("CLM-2");
        assertThat(deserializationResult.event().originalClaimId()).isEqualTo("CLM-1");
    }

    @Test
    void equalsComparesRawBytesByContentNotReference() {
        byte[] bytes1 = {1, 2, 3};
        byte[] bytes2 = {1, 2, 3}; // same content, different array instance

        DeserializationResult result1 = DeserializationResult.failure(bytes1, "bad data");
        DeserializationResult result2 = DeserializationResult.failure(bytes2, "bad data");

        assertThat(result1).isEqualTo(result2);
        assertThat(result1.hashCode()).isEqualTo(result2.hashCode());
    }

    @Test
    void notEqualsWhenRawBytesContentDiffers() {
        DeserializationResult result1 = DeserializationResult.failure(new byte[]{1, 2, 3}, "bad data");
        DeserializationResult result2 = DeserializationResult.failure(new byte[]{4, 5, 6}, "bad data");

        assertThat(result1).isNotEqualTo(result2);
    }

    @Test
    void toStringIncludesActualByteValues() {
        DeserializationResult result = DeserializationResult.failure(new byte[]{1, 2, 3}, "bad data");

        assertThat(result.toString()).contains("[1, 2, 3]");
    }

    @Test
    void notEqualsToDifferentType() {
        DeserializationResult result = DeserializationResult.failure(new byte[]{1, 2, 3}, "bad data");

        assertThat(result.equals("not a DeserializationResult")).isFalse();
    }

    private GenericRecord buildFillRecord(String claimId,
                                          LocalDate fillDate, int daySupply,
                                          BigDecimal quantity,String eventType,
                                          String originalClaimId) {
        Schema quantitySchema = rxFillEventSchema.getField("quantity").schema();
        ByteBuffer quantityBytes = new Conversions.DecimalConversion()
                .toBytes(quantity, quantitySchema, LogicalTypes.decimal(10, 2));
        GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(rxFillEventSchema);
        genericRecordBuilder.set("eventType", new GenericData.EnumSymbol(rxFillEventSchema.getField("eventType").schema(), eventType));
        genericRecordBuilder.set("claimId", claimId);
        genericRecordBuilder.set("memberId", "MBR-1");
        genericRecordBuilder.set("ndcCode", "00093-7424-56");
        genericRecordBuilder.set("fillDate", (int) fillDate.toEpochDay());
        genericRecordBuilder.set("daySupply", daySupply);
        genericRecordBuilder.set("quantity", quantityBytes);
        genericRecordBuilder.set("pharmacyId", "PHM-1");
        genericRecordBuilder.set("rxNumber", "RX-1");
        genericRecordBuilder.set("refillsAuthorized", 3);
        genericRecordBuilder.set("dispensingChannel", new GenericData.EnumSymbol(rxFillEventSchema.getField("dispensingChannel").schema(), "RETAIL"));
        genericRecordBuilder.set("originalClaimId", originalClaimId);
        return genericRecordBuilder.build();
    }


}
