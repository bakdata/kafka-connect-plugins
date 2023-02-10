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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
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

@AllArgsConstructor
public final class FieldDropper {
    private static final Pattern DOT_REGEX = Pattern.compile("\\.");
    private final List<String> exclude;
    private final Cache<? super Schema, Schema> schemaUpdateCache;

    /**
     * Creates a  with a given list of exclude strings
     *
     * @param exclude a list of strings to be dropped in the value
     */
    public static FieldDropper createFieldDropper(final List<String> exclude) {
        return new FieldDropper(exclude, new SynchronizedCache<>(new LRUCache<>(16)));
    }

    /**
     * Creates a default  with no exlcude path.
     */
    public static FieldDropper defaultFieldDropper() {
        return createFieldDropper(Collections.emptyList());
    }

    private static boolean isNotEndOfExcludePath(final Collection<String> excludePath, final String excludeField,
        final Field field) {
        return field.name().equals(excludeField) && excludePath.size() != 1;
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
        final Collection<LinkedList<String>> excludePaths = new ArrayList<>();
        for (final String excludePattern : this.exclude) {
            final LinkedList<String> nestedFields = new LinkedList<>(Arrays.asList(DOT_REGEX.split(excludePattern)));
            excludePaths.add(nestedFields);
        }

        Schema updatedSchema = this.schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = this.makeUpdatedSchema(value.schema(), excludePaths);
            this.schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        return this.getUpdatedStruct(value, updatedSchema, new ArrayList<>(excludePaths));
    }

    private Schema makeUpdatedSchema(final Schema schema, Iterable<? extends LinkedList<String>> excludePaths) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (final LinkedList<String> excludePattern : excludePaths) {
            this.extractSchema(schema, builder, new LinkedList<>(excludePattern));
        }
        return builder.build();
    }

    private Struct getUpdatedStruct(final Struct value, final Schema updatedSchema,
        final Iterable<? extends LinkedList<String>> excludePaths) {
        final Struct updatedValue = new Struct(updatedSchema);
        for (final LinkedList<String> excludePattern : excludePaths) {
            this.updateValues(value, updatedValue, excludePattern);
        }
        return updatedValue;
    }

    private void extractSchema(final Schema schema, final SchemaBuilder schemaBuilder,
        final LinkedList<String> nestedFields) {
        LinkedList<String> excludePath = nestedFields;
        for (final String excludeField : excludePath) {
            final List<String> tmpList = new LinkedList<>(excludePath);
            for (final Field field : schema.schema().fields()) {
                if (isNotEndOfExcludePath(excludePath, excludeField, field)) {
                    excludePath.removeFirst();
                    switch (field.schema().type()) {
                        case ARRAY:
                            final Schema valueSchema = field.schema().valueSchema();
                            final SchemaBuilder arrayStructSchema = SchemaBuilder.struct().name(valueSchema.name());
                            final SchemaBuilder arraySchemaBuilder = SchemaBuilder
                                .array(arrayStructSchema)
                                .name(field.name());
                            this.extractSchema(valueSchema, arrayStructSchema, excludePath);
                            schemaBuilder.field(field.name(), arraySchemaBuilder.build());
                            break;
                        case STRUCT:
                            final SchemaBuilder structSchema = SchemaBuilder.struct().name(field.name());
                            this.extractSchema(field.schema(), structSchema, excludePath);
                            schemaBuilder.field(field.name(), structSchema.schema());
                            break;
                        default:
                            throw new RuntimeException("The given path does not exist");
                    }
                } else if (!field.name().equals(excludeField)) {
                    schemaBuilder.field(field.name(), field.schema());
                }
            }
            excludePath = new LinkedList<>(tmpList);
        }
    }

    private void updateValues(final Struct value, final Struct updatedValue, final LinkedList<String> nestedFields) {
        LinkedList<String> excludePath = nestedFields;
        for (final String excludeField : excludePath) {
            final List<String> tmpList = new LinkedList<>(excludePath);
            for (final Field field : value.schema().fields()) {
                if (isNotEndOfExcludePath(excludePath, excludeField, field)) {
                    switch (field.schema().type()) {
                        case ARRAY:
                            final Iterable<Struct> arrayValues = value.getArray(field.name());
                            final Collection<Struct> values = new ArrayList<>();
                            for (final Struct arrayValue : arrayValues) {
                                final Struct updatedNestedStruct =
                                    new Struct(updatedValue.schema().field(field.name()).schema().valueSchema());
                                excludePath.removeFirst();
                                this.updateValues(arrayValue, updatedNestedStruct, excludePath);
                                excludePath = new LinkedList<>(tmpList);
                                values.add(updatedNestedStruct);
                            }
                            updatedValue.put(field.name(), values);
                            break;
                        case STRUCT:
                            final Struct struct = value.getStruct(field.name());
                            final Struct updatedNestedStruct =
                                new Struct(updatedValue.schema().field(field.name()).schema());
                            excludePath.removeFirst();
                            this.updateValues(struct, updatedNestedStruct, excludePath);
                            excludePath = new LinkedList<>(tmpList);
                            updatedValue.put(field.name(), updatedNestedStruct);
                            break;
                        default:
                            throw new RuntimeException("The given path does not exist");
                    }

                } else if (!field.name().equals(excludeField)) {
                    updatedValue.put(field.name(), value.get(field));
                }
            }
        }
    }
}
