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

import java.util.Collection;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Contains logic for deleting a schema
 */
@AllArgsConstructor
public class DeleteSchema implements NestedIterator {
    private final Path path;
    private Schema oldSchema;
    @Getter
    private SchemaBuilder updatedSchema;

    @Override
    public Collection<Field> fields() {
        return this.oldSchema.schema().fields();
    }

    @Override
    public void onArray(final Field field) {
        final Schema valueSchema = field.schema().valueSchema();
        final String fieldName = field.name();
        final SchemaBuilder arrayStructSchema = SchemaBuilder
            .struct()
            .name(valueSchema.name());
        final SchemaBuilder arraySchemaBuilder = SchemaBuilder
            .array(arrayStructSchema)
            .name(fieldName);
        final Schema upperSchema = this.oldSchema;
        final SchemaBuilder upperSchemaBuilder = this.updatedSchema;
        this.oldSchema = valueSchema;
        this.updatedSchema = arrayStructSchema;
        this.iterate(this.path);
        this.oldSchema = upperSchema;
        this.updatedSchema = upperSchemaBuilder;
        this.updatedSchema.field(fieldName, arraySchemaBuilder.build());
    }

    @Override
    public void onStruct(final Field field) {
        final String fieldName = field.name();
        final SchemaBuilder structSchema = SchemaBuilder.struct().name(fieldName);
        this.oldSchema = field.schema();
        final SchemaBuilder upperSchema = this.updatedSchema;
        this.updatedSchema = structSchema;
        this.iterate(this.path);
        this.updatedSchema = upperSchema;
        this.updatedSchema.field(fieldName, structSchema.schema());
    }

    @Override
    public void onDefault(final Field field) {
        this.updatedSchema.field(field.name(), field.schema());
    }

    @Override
    public void onLastElementPath(final Field field) {
        if (!field.name().equals(this.path.getLastElement())) {
            this.updatedSchema.field(field.name(), field.schema());
        }
    }
}
