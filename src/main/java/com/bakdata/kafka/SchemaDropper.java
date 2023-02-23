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

import static com.bakdata.kafka.Path.createPath;

import java.util.Collections;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

@RequiredArgsConstructor
class SchemaDropper {
    private final Path path;

    static SchemaDropper createSchemaDropper(final List<String> excludePath) {
        return new SchemaDropper(createPath(excludePath, Collections.emptyList()));
    }

    Schema processSchema(final Schema schema) {
        final SchemaBuilder schemaCopy = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        this.addFields(schema, schemaCopy);
        return schemaCopy.build();
    }

    private void addFields(final Schema schema, final SchemaBuilder schemaCopy) {
        schema.fields().forEach(field -> {
            final String fieldName = field.name();
            final Path subPath = this.path.getSubPath(fieldName);
            if (subPath.isInclude()) {
                Schema fieldSchema = field.schema();
                if (subPath.isPrefix()) {
                    final SchemaDropper deleteSchema = new SchemaDropper(subPath);
                    fieldSchema = deleteSchema.transform(field.schema());
                }
                schemaCopy.field(fieldName, fieldSchema);
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
}
