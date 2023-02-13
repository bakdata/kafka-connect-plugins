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

import static com.bakdata.kafka.FieldDropper.createFieldDropper;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public abstract class DropField<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String EXCLUDE_FIELD = "exclude";
    private static final ConfigDef CONFIG_DEF;
    private static final String PURPOSE = "field deletion";

    static {
        final String FIELD_DOCUMENTATION = "Fields to exclude from the resulting Struct.";
        CONFIG_DEF = new ConfigDef()
            .define(EXCLUDE_FIELD, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.MEDIUM,
                FIELD_DOCUMENTATION);
    }

    private FieldDropper fieldDropper;

    @Override
    public void configure(final Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        this.fieldDropper = createFieldDropper(config.getList(EXCLUDE_FIELD));
    }

    @Override
    public R apply(final R inputRecord) {
        if (this.operatingValue(inputRecord) == null) {
            return inputRecord;
        } else if (this.operatingSchema(inputRecord) == null) {
            throw new ConnectException("This SMT can be applied to records with schema.");
        } else {
            return this.applyWithSchema(inputRecord);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        this.fieldDropper = null;
    }

    protected abstract Schema operatingSchema(R inputRecord);

    protected abstract Object operatingValue(R inputRecord);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    private R applyWithSchema(final R inputRecord) {
        final Struct value = requireStruct(this.operatingValue(inputRecord), PURPOSE);

        final Struct updatedValue = this.fieldDropper.updateStruct(value);
        return this.newRecord(inputRecord, updatedValue.schema(), updatedValue);
    }


    /**
     * Implements the method for applying the SMT to the record key.
     */
    public static class Key<R extends ConnectRecord<R>> extends DropField<R> {
        @Override
        protected Schema operatingSchema(final R inputRecord) {
            return inputRecord.keySchema();
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
    public static class Value<R extends ConnectRecord<R>> extends DropField<R> {
        @Override
        protected Schema operatingSchema(final R inputRecord) {
            return inputRecord.valueSchema();
        }

        @Override
        protected Object operatingValue(final R inputRecord) {
            return inputRecord.value();
        }

        @Override
        protected R newRecord(final R inputRecord, final Schema updatedSchema, final Object updatedValue) {
            return inputRecord.newRecord(inputRecord.topic(), inputRecord.kafkaPartition(), inputRecord.keySchema(),
                inputRecord.key(),
                updatedSchema, updatedValue, inputRecord.timestamp());
        }
    }
}
