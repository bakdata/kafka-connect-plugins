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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;
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
@AllArgsConstructor
public final class FieldDropper {
    private static final Pattern DOT_REGEX = Pattern.compile("\\.");
    private final List<String> exclude;
    private final Cache<? super Schema, Schema> schemaUpdateCache;
    private static final int MAX_SIZE = 16;

    /**
     * Creates a  with a given list of exclude strings.
     *
     * @param exclude a list of strings to be dropped in the value
     * @return an instance of the  class with a {@link SynchronizedCache} of size 16.
     */
    public static FieldDropper createFieldDropper(final List<String> exclude) {
        return new FieldDropper(exclude, new SynchronizedCache<>(new LRUCache<>(MAX_SIZE)));
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
        final Collection<Deque<String>> excludePaths = new ArrayList<>();
        for (final String excludePattern : this.exclude) {
            final Deque<String> nestedFields = new ArrayDeque<>(Arrays.asList(DOT_REGEX.split(excludePattern)));
            excludePaths.add(nestedFields);
        }

        Schema updatedSchema = this.schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = this.makeUpdatedSchema(value.schema(), excludePaths);
            this.schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        return this.getUpdatedStruct(value, updatedSchema, excludePaths);
    }

    private Schema makeUpdatedSchema(final Schema schema, final Iterable<? extends Deque<String>> excludePaths) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (final Deque<String> excludePattern : excludePaths) {
            this.extractSchema(schema, builder, excludePattern.size(), excludePattern.peekLast());
        }
        return builder.build();
    }

    private Struct getUpdatedStruct(final Struct value, final Schema updatedSchema,
        final Iterable<? extends Deque<String>> excludePaths) {
        final Struct updatedValue = new Struct(updatedSchema);
        for (final Deque<String> excludePattern : excludePaths) {
            this.updateValues(value, updatedValue, excludePattern.size(), excludePattern.peekLast());
        }
        return updatedValue;
    }

    private void extractSchema(final Schema schema, final SchemaBuilder schemaBuilder,
        final int excludePathDepth, final String lastElementOfExcludePath) {
        int currentPathIndex = excludePathDepth;
        for (final Field field : schema.schema().fields()) {
            if (currentPathIndex != 1) {
                currentPathIndex--;
                switch (field.schema().type()) {
                    case ARRAY:
                        final Schema valueSchema = field.schema().valueSchema();
                        final SchemaBuilder arrayStructSchema = SchemaBuilder.struct().name(valueSchema.name());
                        final SchemaBuilder arraySchemaBuilder = SchemaBuilder
                            .array(arrayStructSchema)
                            .name(field.name());
                        this.extractSchema(valueSchema, arrayStructSchema, currentPathIndex, lastElementOfExcludePath);
                        schemaBuilder.field(field.name(), arraySchemaBuilder.build());
                        break;
                    case STRUCT:
                        final SchemaBuilder structSchema = SchemaBuilder.struct().name(field.name());
                        this.extractSchema(field.schema(), structSchema, currentPathIndex, lastElementOfExcludePath);
                        schemaBuilder.field(field.name(), structSchema.schema());
                        break;
                    default:
                        schemaBuilder.field(field.name(), field.schema());
                }
                currentPathIndex++;
            } else if (!field.name().equals(lastElementOfExcludePath)) {
                schemaBuilder.field(field.name(), field.schema());
            }
        }
    }

    private void updateValues(final Struct value, final Struct updatedValue, final int excludePathDepth,
        final String lastElementOfExcludePath) {
        int currentPathIndex = excludePathDepth;
        for (final Field field : value.schema().fields()) {
            if (currentPathIndex != 1) {
                currentPathIndex--;
                switch (field.schema().type()) {
                    case ARRAY:
                        final Iterable<Struct> arrayValues = value.getArray(field.name());
                        final Collection<Struct> updatedArrayValues =
                            this.addArrayValues(updatedValue, lastElementOfExcludePath, currentPathIndex, field,
                                arrayValues);
                        updatedValue.put(field.name(), updatedArrayValues);
                        break;
                    case STRUCT:
                        final Struct struct = value.getStruct(field.name());
                        final Struct updatedNestedStruct =
                            new Struct(updatedValue.schema().field(field.name()).schema());
                        this.updateValues(struct, updatedNestedStruct, currentPathIndex, lastElementOfExcludePath);
                        updatedValue.put(field.name(), updatedNestedStruct);
                        break;
                    default:
                        updatedValue.put(field.name(), value.get(field));
                }
                currentPathIndex++;
            } else if (!field.name().equals(lastElementOfExcludePath)) {
                updatedValue.put(field.name(), value.get(field));
            }
        }
    }

    private Collection<Struct> addArrayValues(final Struct updatedValue, final String lastElementOfExcludePath,
        final int currentPathIndex,
        final Field field, final Iterable<? extends Struct> arrayValues) {
        final Collection<Struct> values = new ArrayList<>();
        for (final Struct arrayValue : arrayValues) {
            final Struct updatedNestedStruct =
                new Struct(updatedValue.schema().field(field.name()).schema().valueSchema());
            this.updateValues(arrayValue, updatedNestedStruct, currentPathIndex,
                lastElementOfExcludePath);
            values.add(updatedNestedStruct);
        }
        return values;
    }
}
