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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public abstract class Convert<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String CONVERTER_FIELD = "converter";
    private static final String FIELD_DOCUMENTATION = "Converter to apply to input.";

    private static final Set<Schema> schemaSet = Set.of(Schema.OPTIONAL_BYTES_SCHEMA, Schema.BYTES_SCHEMA);
    private static final ConfigDef CONFIG_DEF =
        new ConfigDef().define(CONVERTER_FIELD, Type.CLASS, ByteArrayConverter.class, Importance.HIGH,
            FIELD_DOCUMENTATION);
    private Converter converter;

    @Override
    public void configure(final Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        final Map<String, Object> converterConfigs = new HashMap<>(configs);
        converterConfigs.put(ConverterConfig.TYPE_CONFIG, this.converterType().getName());
        this.converter = config.getConfiguredInstance(CONVERTER_FIELD, Converter.class, converterConfigs);
    }

    @Override
    public R apply(final R inputRecord) {
        if (this.operatingValue(inputRecord) == null) {
            return inputRecord;
        }
        final Schema schema = this.operatingSchema(inputRecord);
        if (schemaSet.contains(schema)) {
            final byte[] value = (byte[]) this.operatingValue(inputRecord);
            final SchemaAndValue schemaAndValue = this.converter.toConnectData(inputRecord.topic(), value);
            return this.newRecord(inputRecord, schemaAndValue.schema(), schemaAndValue.value());
        } else {
            throw new ConnectException("Unsupported Schema " + schema);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        this.converter = null;
    }

    protected abstract Schema operatingSchema(R inputRecord);

    protected abstract Object operatingValue(R inputRecord);

    protected abstract ConverterType converterType();

    protected abstract R newRecord(R inputRecord, Schema updatedSchema, Object updatedValue);

    /**
     * Implements the method for applying the SMT to the record key.
     */
    public static class Key<R extends ConnectRecord<R>> extends Convert<R> {
        @Override
        protected Schema operatingSchema(final R inputRecord) {
            return inputRecord.keySchema();
        }

        @Override
        protected ConverterType converterType() {
            return ConverterType.KEY;
        }

        @Override
        protected Object operatingValue(final R inputRecord) {
            return inputRecord.key();
        }

        @Override
        protected R newRecord(final R inputRecord, final Schema updatedSchema, final Object updatedValue) {
            return inputRecord.newRecord(inputRecord.topic(), inputRecord.kafkaPartition(), updatedSchema, updatedValue,
                inputRecord.valueSchema(), inputRecord.value(), inputRecord.timestamp());
        }
    }

    /**
     * Implements the method for applying the SMT to the record value.
     */
    public static class Value<R extends ConnectRecord<R>> extends Convert<R> {
        @Override
        protected Schema operatingSchema(final R inputRecord) {
            return inputRecord.valueSchema();
        }

        @Override
        protected Object operatingValue(final R inputRecord) {
            return inputRecord.value();
        }

        @Override
        protected ConverterType converterType() {
            return ConverterType.VALUE;
        }

        @Override
        protected R newRecord(final R inputRecord, final Schema updatedSchema, final Object updatedValue) {
            return inputRecord.newRecord(inputRecord.topic(), inputRecord.kafkaPartition(), inputRecord.keySchema(),
                inputRecord.key(),
                updatedSchema, updatedValue, inputRecord.timestamp());
        }
    }
}
