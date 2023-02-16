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

import java.util.List;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
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
    private final Exclude excludePaths;

    /**
     * Creates a  with a given list of exclude strings.
     *
     * @param exclude a list of strings to be dropped in the value
     * @return an instance of the  class with a {@link SynchronizedCache} of size 16.
     */
    public static FieldDropper createFieldDropper(final List<String> exclude) {
        final Exclude excludes = createListExclude(exclude);
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
            updatedSchema = makeUpdatedSchema(value.schema(), this.excludePaths);
            this.schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        return getUpdatedStruct(value, updatedSchema, this.excludePaths);
    }

    private static Schema makeUpdatedSchema(final Schema schema, final Exclude excludePaths) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        final DeleteSchema deleteSchema = new DeleteSchema(excludePaths, schema, builder);
        deleteSchema.iterate();
        return deleteSchema.getUpdatedSchema().build();
    }

    private static Struct getUpdatedStruct(final Struct value, final Schema updatedSchema,
        final Exclude excludePaths) {
        final Struct updatedValue = new Struct(updatedSchema);
        final DeleteValue deleteValue = new DeleteValue(excludePaths, value, updatedValue);
        deleteValue.iterate();
        return deleteValue.getUpdatedValue();
    }
}
