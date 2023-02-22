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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

@RequiredArgsConstructor
class SchemaDropper {
    private final List<String> excludePath;
    private final List<String> currentPath;

    static SchemaDropper createSchemaDropper(final String exclude) {
        final List<String> excludePath = Path.split(exclude);
        return new SchemaDropper(excludePath, Collections.emptyList());
    }

    public Schema processSchema(final Schema schema) {
        final SchemaBuilder schemaCopy = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        this.addFields(schema, schemaCopy);
        return schemaCopy.build();
    }

    private void addFields(final Schema schema, final SchemaBuilder schemaCopy) {
        schema.fields().forEach(field -> {
            final String fieldName = field.name();
            final List<String> subPath = this.getSubPath(fieldName);
            if (!this.isExclude(subPath)) {
                final SchemaDropper deleteSchema = new SchemaDropper(this.excludePath, subPath);
                schemaCopy.field(fieldName, deleteSchema.transform(field.schema()));
            }
        });
    }

    private Schema transform(final Schema schema) {
        switch (schema.type()) {
            case MAP:
                return this.processMap(schema);
            case ARRAY:
                return this.processArray(schema);
            case STRUCT:
                return this.processSchema(schema);
            default:
                return schema;
        }
    }

    private Schema processMap(final Schema schema) {
        final Schema updatedValueSchema = this.transform(schema.valueSchema());
        final Schema updatedKeySchema = this.transform(schema.keySchema());
        return SchemaBuilder
            .map(updatedKeySchema, updatedValueSchema)
            .name(schema.name())
            .build();
    }

    private Schema processArray(final Schema schema) {
        final Schema updatedSchema = this.transform(schema.valueSchema());
        return SchemaBuilder
            .array(updatedSchema)
            .name(schema.name())
            .build();
    }

    private boolean isExclude(final List<String> strings) {
        return this.excludePath.equals(strings);
    }

    private List<String> getSubPath(final String fieldName) {
        final List<String> strings = new ArrayList<>(this.currentPath);
        strings.add(fieldName);
        return strings;
    }
}
