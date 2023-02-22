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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

@RequiredArgsConstructor
class StructDropper {
    private final List<String> excludePath;
    private final List<String> currentPath;

    static StructDropper createStructDropper(final String exclude) {
        final List<String> excludePath = Path.split(exclude);
        return new StructDropper(excludePath, Collections.emptyList());
    }

    Struct processStruct(final Struct struct, final Schema updatedSchema) {
        final Struct structCopy = new Struct(updatedSchema);
        structCopy.schema().fields().forEach(field -> {
            final String fieldName = field.name();
            final List<String> subPath = this.getSubPath(fieldName);
            if (!this.isExclude(subPath)) {
                final StructDropper deleteSchema = new StructDropper(this.excludePath, subPath);
                final Object transform = deleteSchema.transform(struct.get(fieldName), field.schema());
                structCopy.put(fieldName, transform);
            }
        });
        return structCopy;
    }

    private Object transform(final Object struct, final Schema schema) {
        switch (schema.type()) {
            case ARRAY:
                return this.processArray((List<Object>) struct, schema.valueSchema());
            case MAP:
                return this.processMap((Map<Object, Object>) struct, schema.valueSchema());
            case STRUCT:
                return this.processStruct((Struct) struct, schema);
            default:
                return struct;
        }
    }

    private Map<Object, Object> processMap(final Map<Object, Object> value, final Schema schema) {
        final Map<Object, Object> mapValues = new HashMap<>();

        for (final Entry<Object, Object> entry : value.entrySet()) {
            mapValues.put(entry.getKey(), this.transform(entry.getValue(), schema));
        }
        return mapValues;
    }

    private List<Object> processArray(final Iterable<Object> value, final Schema schema) {
        final List<Object> arrayValues = new ArrayList<>();

        for (final Object arrayValue : value) {
            arrayValues.add(this.transform(arrayValue, schema));
        }
        return arrayValues;
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