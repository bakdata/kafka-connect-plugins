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

import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

@RequiredArgsConstructor
final class StructFieldDropper {
    private static final int CACHE_SIZE = 16;
    private final Cache<? super Schema, Schema> schemaUpdateCache;
    private final Path dropPath;

    static StructFieldDropper createStructFieldDropper(final String exclude) {
        final Path path = createPath(exclude);
        return new StructFieldDropper(new SynchronizedCache<>(new LRUCache<>(CACHE_SIZE)), path);
    }

    private static Schema makeUpdatedSchema(final Schema schema, final Path excludePaths) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        final DeleteSchema deleteSchema = new DeleteSchema(excludePaths, schema, builder);
        deleteSchema.iterate(excludePaths);
        return Objects.requireNonNull(deleteSchema).getUpdatedSchema().build();
    }

    private static Struct getUpdatedStruct(final Struct value, final Schema updatedSchema, final Path excludePath) {
        final Struct updatedValue = new Struct(updatedSchema);
        final DeleteStructValue deleteStructValue = new DeleteStructValue(excludePath, value, updatedValue);
        deleteStructValue.iterate(excludePath);
        return Objects.requireNonNull(deleteStructValue).getUpdatedValue();
    }

    Struct updateStruct(final Struct value) {
        Schema updatedSchema = this.schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema(), this.dropPath);
            this.schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        return getUpdatedStruct(value, updatedSchema, this.dropPath);
    }
}
