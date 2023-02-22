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

import static com.bakdata.kafka.Convert.CONVERTER_FIELD;
import static com.bakdata.kafka.DropField.EXCLUDE_FIELD;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.bakdata.kafka.Convert.Key;
import com.bakdata.kafka.Convert.Value;
import com.bakdata.test.smt.PrimitiveObject;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.StringConverter;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class ConvertTest {

    private static final String TEST_TOPIC = "test-topic";
    private static final String MESSAGE = "Schema should not be null.";
    @InjectSoftAssertions
    private SoftAssertions softly;

    private static SinkRecord getSinkRecord(final Schema keySchema, final Object keyValue, final Schema valueSchema,
        final Object valueValue) {
        return new SinkRecord(TEST_TOPIC, 0, keySchema, keyValue, valueSchema, valueValue, 0);
    }

    @Test
    void shouldReturnInputRecordWhenValueIsNull() {
        final SinkRecord sinkRecord = getSinkRecord(null, "testKey".getBytes(StandardCharsets.UTF_8),
            Schema.BYTES_SCHEMA,
            null);
        try (final Convert<SinkRecord> convert = new Value<>()) {
            convert.configure(Map.of(CONVERTER_FIELD, StringConverter.class));
            final SinkRecord newRecord = convert.apply(sinkRecord);
            this.softly.assertThat(newRecord.key()).isEqualTo("testKey".getBytes(StandardCharsets.UTF_8));
            this.softly.assertThat(newRecord.value()).isNull();
        }
    }

    @Test
    void shouldConvertValue() {
        final SinkRecord sinkRecord = getSinkRecord(null, "testKey".getBytes(StandardCharsets.UTF_8),
            Schema.BYTES_SCHEMA, "testValue".getBytes(StandardCharsets.UTF_8));
        try (final Convert<SinkRecord> convert = new Value<>()) {
            convert.configure(Map.of(CONVERTER_FIELD, StringConverter.class));
            final SinkRecord converted = convert.apply(sinkRecord);
            this.softly.assertThat(converted.key()).isEqualTo("testKey".getBytes(StandardCharsets.UTF_8));
            this.softly.assertThat(converted.value()).isEqualTo("testValue");
        }
    }

    @Test
    void shouldConvertKey() {
        final SinkRecord sinkRecord =
            getSinkRecord(Schema.OPTIONAL_BYTES_SCHEMA, "testKey".getBytes(StandardCharsets.UTF_8),
                Schema.BYTES_SCHEMA, "testValue".getBytes(StandardCharsets.UTF_8));

        try (final Convert<SinkRecord> convert = new Key<>()) {
            convert.configure(Map.of(CONVERTER_FIELD, StringConverter.class));
            final SinkRecord converted = convert.apply(sinkRecord);
            this.softly.assertThat(converted.key()).isEqualTo("testKey");
            this.softly.assertThat(converted.value()).isEqualTo("testValue".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    void shouldRaiseExceptionIfSchemaRecordDoesNotHaveSchema() {
        final SinkRecord sinkRecord = getSinkRecord(null, "testKey".getBytes(StandardCharsets.UTF_8),
            null, "testValue".getBytes(StandardCharsets.UTF_8));
        try (final Convert<SinkRecord> convert = new Value<>()) {
            convert.configure(Map.of(CONVERTER_FIELD, StringConverter.class));
            assertThatThrownBy(() -> convert.apply(sinkRecord)).isInstanceOf(ConnectException.class)
                .hasMessage("Schema should not be null.");
        }
    }

    @Test
    void shouldRaiseExceptionIfSchemaTypeIsNotBytes() {
        final PrimitiveObject primitiveObject = new PrimitiveObject("dropped_field", 1234);
        final SinkRecord sinkRecord = getSinkRecord(null, "testKey".getBytes(StandardCharsets.UTF_8),
            Schema.INT32_SCHEMA, "testValue".getBytes(StandardCharsets.UTF_8));
        try (final Convert<SinkRecord> convert = new Value<>()) {
            convert.configure(Map.of(CONVERTER_FIELD, StringConverter.class));
            assertThatThrownBy(() -> convert.apply(sinkRecord)).isInstanceOf(ConnectException.class)
                .hasMessage("Unsupported Schema " + Schema.INT32_SCHEMA);
        }
    }
}
