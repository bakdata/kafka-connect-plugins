/*
 * MIT License
 *
 * Copyright (c) 2025 bakdata
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

import com.bakdata.schemaregistrymock.junit5.SchemaRegistryMockExtension;
import com.bakdata.test.smt.NullableObject;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.StringConverter;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(SoftAssertionsExtension.class)
class NullPredicateTest {
    private static final String TEST_TOPIC = "test-topic";
    @RegisterExtension
    final SchemaRegistryMockExtension schemaRegistryMock = new SchemaRegistryMockExtension();
    @InjectSoftAssertions
    private SoftAssertions softly;

    private static SinkRecord getSinkRecord(final Schema valueSchema, final Object valueValue) {
        return new SinkRecord(
                TEST_TOPIC,
                0,
                null,
                "testKey".getBytes(StandardCharsets.UTF_8),
                valueSchema,
                valueValue,
                0
        );
    }

    private static NullableObject.Builder defaultObject() {
        return NullableObject.newBuilder()
                .setField1("field1")
                .setField2("field2");
    }

    @Test
    void shouldBeFalseForNestedNonNullValue() {
        final SchemaAndValue schemaAndValue = this.getSchemaAndValue(defaultObject().build());
        final SinkRecord sinkRecord = getSinkRecord(schemaAndValue.schema(), schemaAndValue.value());
        try (final NullPredicate<SinkRecord> nullPredicate = new NullPredicate<>()) {
            nullPredicate.configure(Map.of(NullPredicate.FIELD_PATH, "field2"));
            this.softly.assertThat(nullPredicate.test(sinkRecord)).isFalse();
        }
    }

    @Test
    void shouldBeTrueForNestedNullValue() {
        final SchemaAndValue schemaAndValue = this.getSchemaAndValue(defaultObject().clearField2().build());
        final SinkRecord sinkRecord = getSinkRecord(schemaAndValue.schema(), schemaAndValue.value());
        try (final NullPredicate<SinkRecord> nullPredicate = new NullPredicate<>()) {
            nullPredicate.configure(Map.of(NullPredicate.FIELD_PATH, "field2"));
            this.softly.assertThat(nullPredicate.test(sinkRecord)).isTrue();
        }
    }

    @Test
    void shouldBeFalseForRootNonNullValue() {
        final SchemaAndValue schemaAndValue = this.getSchemaAndValue(defaultObject().build());
        final SinkRecord sinkRecord = getSinkRecord(schemaAndValue.schema(), schemaAndValue.value());
        try (final NullPredicate<SinkRecord> nullPredicate = new NullPredicate<>()) {
            this.softly.assertThat(nullPredicate.test(sinkRecord)).isFalse();
        }
    }

    @Test
    void shouldBeFalseForRootPrimitiveNonNullValue() {
        final StringConverter stringConverter = new StringConverter();
        final SchemaAndValue connectData = stringConverter.toConnectData(TEST_TOPIC, "test".getBytes(StandardCharsets.UTF_8));
        final SinkRecord sinkRecord = getSinkRecord(connectData.schema(), connectData.value());
        try (final NullPredicate<SinkRecord> nullPredicate = new NullPredicate<>()) {
            this.softly.assertThat(nullPredicate.test(sinkRecord)).isFalse();
        }
    }

    @Test
    void shouldBeTrueForRootPrimitiveNullValue() {
        final StringConverter stringConverter = new StringConverter();
        final SchemaAndValue connectData = stringConverter.toConnectData(TEST_TOPIC, null);
        final SinkRecord sinkRecord = getSinkRecord(connectData.schema(), connectData.value());
        try (final NullPredicate<SinkRecord> nullPredicate = new NullPredicate<>()) {
            this.softly.assertThat(nullPredicate.test(sinkRecord)).isTrue();
        }
    }

    @Test
    void shouldThrowForPrimitiveWithField() {
        final StringConverter stringConverter = new StringConverter();
        final SchemaAndValue connectData = stringConverter.toConnectData(TEST_TOPIC, "test".getBytes(StandardCharsets.UTF_8));
        final SinkRecord sinkRecord = getSinkRecord(connectData.schema(), connectData.value());
        try (final NullPredicate<SinkRecord> nullPredicate = new NullPredicate<>()) {
            nullPredicate.configure(Map.of(NullPredicate.FIELD_PATH, "field2"));
            this.softly.assertThatException().isThrownBy(() -> nullPredicate.test(sinkRecord))
                    .withMessage(
                            "Only Struct objects supported for [extracting field for field 'field2'], found: java.lang"
                                    + ".String");
        }
    }

    private <T extends SpecificRecord> SchemaAndValue getSchemaAndValue(final T value) {
        final Map<String, String> schemaRegistryUrlConfig = Map
                .of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistryMock.getUrl());
        try (final Serializer<T> serializer = new SpecificAvroSerializer<>()) {
            final Converter avroConverter = new AvroConverter();
            avroConverter.configure(schemaRegistryUrlConfig, false);
            serializer.configure(schemaRegistryUrlConfig, false);
            final byte[] valueBytes = serializer.serialize(TEST_TOPIC, value);
            return avroConverter.toConnectData(TEST_TOPIC, valueBytes);
        }
    }

}
