/*
 * MIT License
 *
 * Copyright (c) 2023 bakdata
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.bakdata.kafka;

import static com.bakdata.kafka.DropField.EXCLUDE_FIELD;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.bakdata.kafka.DropField.Key;
import com.bakdata.kafka.DropField.Value;
import com.bakdata.kafka.util.User;
import com.bakdata.schemaregistrymock.junit5.SchemaRegistryMockExtension;
import com.bakdata.test.smt.NestedObject;
import com.bakdata.test.smt.PrimitiveObject;
import com.bakdata.test.smt.RecordCollection;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(SoftAssertionsExtension.class)
class DropFieldTest {
    private static final String TEST_TOPIC = "test-topic";
    private static final String MESSAGE = "This SMT can be applied only to records with schema.";
    private final ObjectMapper mapper = new ObjectMapper();
    @RegisterExtension
    final SchemaRegistryMockExtension schemaRegistryMock = new SchemaRegistryMockExtension();
    @InjectSoftAssertions
    private SoftAssertions softly;

    private static SinkRecord getSinkRecord(final Schema keySchema, final Object keyValue, final Schema valueSchema,
        final Object valueValue) {
        return new SinkRecord(TEST_TOPIC, 0, keySchema, keyValue, valueSchema, valueValue, 0);
    }

    private static RecordCollection createComplexKey() {
        final PrimitiveObject primitiveObject = PrimitiveObject.newBuilder()
            .setDroppedField("This field will also be dropped.")
            .setKeptField(1234)
            .build();
        final NestedObject nestedObject = new NestedObject(primitiveObject, true);

        final PrimitiveObject primitiveObject2 = PrimitiveObject.newBuilder()
            .setDroppedField("This field will also be dropped.")
            .setKeptField(5678)
            .build();
        final NestedObject nestedObject2 = new NestedObject(primitiveObject2, false);
        return new RecordCollection(List.of(nestedObject, nestedObject2));
    }

    @Test
    void shouldReturnInputRecordWhenValueIsNull() {
        final SchemaAndValue schemaAndValue = this.getSchemaAndValue(false, null);
        final SinkRecord sinkRecord = getSinkRecord(null, "testKey".getBytes(StandardCharsets.UTF_8),
            schemaAndValue.schema(),
            schemaAndValue.value());
        try (final DropField<SinkRecord> dropField = new Value<>()) {
            dropField.configure(Map.of(EXCLUDE_FIELD, "some.random.field"));
            final SinkRecord newRecord = dropField.apply(sinkRecord);
            this.softly.assertThat(newRecord.key()).isEqualTo("testKey".getBytes(StandardCharsets.UTF_8));
            this.softly.assertThat(newRecord.value()).isNull();
        }
    }

    @Test
    void shouldReturnInputRecordWhenKeyIsNull() {
        final SchemaAndValue schemaAndValue = this.getSchemaAndValue(true, null);
        final SinkRecord sinkRecord = getSinkRecord(schemaAndValue.schema(),
            schemaAndValue.value(), null, "testKey".getBytes(StandardCharsets.UTF_8));
        try (final DropField<SinkRecord> dropField = new Key<>()) {
            dropField.configure(Map.of(EXCLUDE_FIELD, "some.random.field"));
            final SinkRecord newRecord = dropField.apply(sinkRecord);
            this.softly.assertThat(newRecord.key()).isNull();
            this.softly.assertThat(newRecord.value()).isEqualTo("testKey".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    void shouldDropFieldIfValueIsJsonString() throws JsonProcessingException {
        final String value = this.createValue();
        final SinkRecord sinkRecord = getSinkRecord(null, "test", Schema.STRING_SCHEMA, value);
        try (final DropField<SinkRecord> dropField = new Value<>()) {
            dropField.configure(Map.of(EXCLUDE_FIELD, "lastName"));
            final SinkRecord newRecord = dropField.apply(sinkRecord);
            this.softly.assertThat(newRecord.key()).isEqualTo("test");
            this.softly.assertThat(newRecord.value()).isEqualTo("{\"firstName\":\"Jack\",\"age\":25}");
        }
    }

    @Test
    void shouldThrowExceptionWhenValueDoesNotHaveSchema() {
        final PrimitiveObject keyObject = new PrimitiveObject("test", 1234);
        final SchemaAndValue schemaAndValue = this.getSchemaAndValue(true, keyObject);
        final SinkRecord sinkRecord = getSinkRecord(schemaAndValue.schema(),
            schemaAndValue.value(), null, "testKey".getBytes(StandardCharsets.UTF_8));
        try (final DropField<SinkRecord> dropField = new Value<>()) {
            dropField.configure(Map.of(EXCLUDE_FIELD, "some.random.field"));
            assertThatThrownBy(() -> dropField.apply(sinkRecord)).isInstanceOf(ConnectException.class)
                .hasMessage(MESSAGE);
        }
    }

    @Test
    void shouldThrowExceptionWhenKeyDoesNotHaveSchema() {
        final PrimitiveObject valueObject = new PrimitiveObject("test", 1234);
        final SchemaAndValue schemaAndValue = this.getSchemaAndValue(false, valueObject);
        final SinkRecord sinkRecord = getSinkRecord(null, "testKey".getBytes(StandardCharsets.UTF_8),
            schemaAndValue.schema(),
            schemaAndValue.value());
        try (final DropField<SinkRecord> dropField = new Key<>()) {
            dropField.configure(Map.of(EXCLUDE_FIELD, "some.random.field"));
            assertThatThrownBy(() -> dropField.apply(sinkRecord))
                .isInstanceOf(ConnectException.class)
                .hasMessage(MESSAGE);
        }
    }

    @Test
    void shouldDropNestedValueFromKey() {
        final RecordCollection complexKey = createComplexKey();
        final SchemaAndValue schemaAndValue = this.getSchemaAndValue(true, complexKey);
        final SinkRecord sinkRecord = getSinkRecord(schemaAndValue.schema(),
            schemaAndValue.value(), null, "testKey".getBytes(StandardCharsets.UTF_8));
        try (final DropField<SinkRecord> dropField = new Key<>()) {
            dropField.configure(Map.of(EXCLUDE_FIELD, "collections.complex_object.dropped_field"));
            final SinkRecord newRecord = dropField.apply(sinkRecord);
            this.softly.assertThat(newRecord.key())
                .isInstanceOfSatisfying(Struct.class,
                    newKey -> this.softly.assertThat(newKey.getArray("collections"))
                        .hasSize(2)
                        .satisfies(array -> {
                            this.softly.assertThat(array)
                                .first()
                                .isInstanceOfSatisfying(Struct.class, struct -> {
                                    final Integer fieldValue = struct
                                        .getStruct("complex_object")
                                        .getInt32("kept_field");
                                    this.softly.assertThat(fieldValue).isEqualTo(1234);
                                    this.softly.assertThat(struct.getBoolean("boolean_field")).isTrue();
                                });
                            this.softly.assertThat(array.get(1))
                                .isInstanceOfSatisfying(Struct.class, struct -> {
                                    final Integer fieldValue = struct
                                        .getStruct("complex_object")
                                        .getInt32("kept_field");
                                    this.softly.assertThat(fieldValue).isEqualTo(5678);

                                    this.softly.assertThat(struct.getBoolean("boolean_field"))
                                        .isFalse();
                                });
                        }).allSatisfy(array -> this.softly.assertThat(array)
                            .isInstanceOfSatisfying(Struct.class, struct -> {
                                final Field field = struct
                                    .getStruct("complex_object")
                                    .schema()
                                    .field("dropped_field");
                                this.softly.assertThat(field).isNull();
                            })));
            this.softly.assertThat(newRecord.value()).isEqualTo("testKey".getBytes(StandardCharsets.UTF_8));
        }
    }

    private <T extends SpecificRecord> SchemaAndValue getSchemaAndValue(final boolean isKey, final T primitiveObject) {
        final Map<String, String> schemaRegistryUrlConfig = Map
            .of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistryMock.getUrl());
        try (final Serializer<T> serializer = new SpecificAvroSerializer<>()) {
            final Converter avroConverter = new AvroConverter();
            avroConverter.configure(schemaRegistryUrlConfig, isKey);
            serializer.configure(schemaRegistryUrlConfig, isKey);
            final byte[] valueBytes = serializer.serialize(TEST_TOPIC, primitiveObject);
            return avroConverter.toConnectData(TEST_TOPIC, valueBytes);
        }
    }

    private String createValue() throws JsonProcessingException {
        final User user = new User("Jack", "Java", 25);
        final JsonNode newNode = this.mapper.convertValue(user, JsonNode.class);

        return this.mapper.writeValueAsString(newNode);
    }
}
