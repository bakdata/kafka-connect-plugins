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

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/**
 * Predicate testing that the value or a field of the value is null.
 */
public class NullPredicate<R extends ConnectRecord<R>> implements Predicate<R> {

    public static final String FIELD_PATH = "field";
    private static final String FIELD_DOCUMENTATION =
            "Name of the field to check for null. If not provided, the whole value is checked.";
    private static final String DEFAULT_VALUE = "";
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_PATH, Type.STRING, DEFAULT_VALUE, Importance.MEDIUM, FIELD_DOCUMENTATION);
    private String fieldPath = DEFAULT_VALUE;

    @Override
    public boolean test(final R record) {
        if (this.fieldPath.isEmpty()) {
            return record.value() == null;
        }

        final Struct value =
                requireStruct(record.value(), String.format("extracting field for field '%s'", this.fieldPath));
        final Field field = value.schema().field(this.fieldPath);
        if (field == null) {
            throw new IllegalArgumentException(String.format("Field %s not part of value's schema", this.fieldPath));
        }

        return value.get(this.fieldPath) == null;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        this.fieldPath = config.getString(FIELD_PATH);
    }

    @Override
    public void close() {
        // do nothing
    }

}
