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

import static com.bakdata.kafka.Exclude.createListExclude;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

/**
 * Contains the logic of excluding the fields in the schema and value.
 */
@RequiredArgsConstructor
public final class FieldDropper {
    private static final Pattern DOT_REGEX = Pattern.compile("\\.");
    private static final int CACHE_SIZE = 16;
    private final List<String> exclude;
    private final Cache<? super Schema, Schema> schemaUpdateCache;
    private final Iterable<Exclude> excludePaths;

    /**
     * Creates a  with a given list of exclude strings.
     *
     * @param exclude a list of strings to be dropped in the value
     * @return an instance of the  class with a {@link SynchronizedCache} of size 16.
     */
    public static FieldDropper createFieldDropper(final List<String> exclude) {
        final Iterable<Exclude> excludes = createListExclude(exclude);
        return new FieldDropper(exclude, new SynchronizedCache<>(new LRUCache<>(CACHE_SIZE)), excludes);
    }


    /**
     * This method creates the updated schema and then inserts the values based on the give exclude paths.
     *
     * @param value Old value
     * @return Updated value with the excluded field(s)
     */
    public Struct updateStruct(final Struct value) {
        if (this.exclude.isEmpty()) {
            return value;
        }

        Schema updatedSchema = this.schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = this.makeUpdatedSchema(value.schema(), this.excludePaths);
            this.schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        return this.getUpdatedStruct(value, updatedSchema, this.excludePaths);
    }

    private Schema makeUpdatedSchema(final Schema schema, final Iterable<Exclude> excludePaths) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (final Exclude excludePattern : excludePaths) {
            this.extractSchema(schema, builder, excludePattern);
        }
        return builder.build();
    }

    private Struct getUpdatedStruct(final Struct value, final Schema updatedSchema,
        final Iterable<Exclude> excludePaths) {
        final Struct updatedValue = new Struct(updatedSchema);
        for (final Exclude exclude : excludePaths) {
            this.updateValues(value, updatedValue, exclude);
        }
        return updatedValue;
    }

    private void extractSchema(final Schema oldSchema, final SchemaBuilder updatedSchema, final Exclude exclude) {
        final int currentPathIndex = exclude.getDepth();
        final String lastElement = exclude.getLastElement();
        for (final Field field : oldSchema.schema().fields()) {
            final String fieldName = field.name();
            final Schema fieldSchema = field.schema();
            if (currentPathIndex != 1) {
                exclude.moveDeeperIntoPath();
                switch (fieldSchema.type()) {
                    case ARRAY:
                        final Schema valueSchema = fieldSchema.valueSchema();
                        final SchemaBuilder arrayStructSchema = SchemaBuilder.struct().name(valueSchema.name());
                        final SchemaBuilder arraySchemaBuilder = SchemaBuilder
                            .array(arrayStructSchema)
                            .name(fieldName);
                        this.extractSchema(valueSchema, arrayStructSchema, exclude);
                        updatedSchema.field(fieldName, arraySchemaBuilder.build());
                        break;
                    case STRUCT:
                        final SchemaBuilder structSchema = SchemaBuilder.struct().name(fieldName);
                        this.extractSchema(fieldSchema, structSchema, exclude);
                        updatedSchema.field(fieldName, structSchema.schema());
                        break;
                    default:
                        updatedSchema.field(fieldName, fieldSchema);
                }
                exclude.moveHigherIntoPath();
            } else if (!fieldName.equals(lastElement)) {
                updatedSchema.field(fieldName, fieldSchema);
            }
        }
    }

    private void updateValues(final Struct oldValue, final Struct updatedValue, final Exclude exclude) {
        final int currentPathIndex = exclude.getDepth();
        final String lastElement = exclude.getLastElement();
        for (final Field field : oldValue.schema().fields()) {
            final String fieldName = field.name();
            if (currentPathIndex != 1) {
                exclude.moveDeeperIntoPath();
                switch (field.schema().type()) {
                    case ARRAY:
                        final Iterable<Struct> arrayValues = oldValue.getArray(fieldName);
                        final Collection<Struct> updatedArrayValues =
                            this.addArrayValues(updatedValue, field, arrayValues, exclude);
                        updatedValue.put(fieldName, updatedArrayValues);
                        break;
                    case STRUCT:
                        final Struct struct = oldValue.getStruct(fieldName);
                        final Struct updatedNestedStruct = new Struct(updatedValue.schema().field(fieldName).schema());
                        this.updateValues(struct, updatedNestedStruct, exclude);
                        updatedValue.put(fieldName, updatedNestedStruct);
                        break;
                    default:
                        updatedValue.put(fieldName, oldValue.get(field));
                }
                exclude.moveHigherIntoPath();
            } else if (!fieldName.equals(lastElement)) {
                updatedValue.put(fieldName, oldValue.get(field));
            }
        }
    }

    private Collection<Struct> addArrayValues(final Struct updatedValue,
        final Field field, final Iterable<? extends Struct> arrayValues, final Exclude exclude) {
        final Collection<Struct> values = new ArrayList<>();
        for (final Struct arrayValue : arrayValues) {
            final Struct updatedNestedStruct =
                new Struct(updatedValue.schema().field(field.name()).schema().valueSchema());
            this.updateValues(arrayValue, updatedNestedStruct, exclude);
            values.add(updatedNestedStruct);
        }
        return values;
    }
}
