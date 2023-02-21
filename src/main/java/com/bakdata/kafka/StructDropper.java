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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class StructDropper {
    private final List<String> excludePath;
    private final List<String> currentPath;

    static StructDropper createStructDropper(final String exclude) {
        final List<String> excludePath = Path.split(exclude);
        return new StructDropper(excludePath, Collections.emptyList());
    }

    Struct processStruct(final Struct struct, final Schema updatedSchema) {
        Struct structCopy = new Struct(updatedSchema);
        structCopy.schema().fields().forEach(field -> {
            final String fieldName = field.name();
            final List<String> subPath = this.getSubPath(fieldName);
            if (!this.isExclude(subPath)) {
                final StructDropper deleteSchema = new StructDropper(this.excludePath, subPath);
                Object struct1 = struct.get(fieldName);
                Object transform = deleteSchema.transform(struct1, fieldName);
                structCopy.put(fieldName, transform);
            }
        });
//        addFields(struct, structCopy);
        return structCopy;
    }

//    private void addFields(final Struct struct, final Struct structCopy) {
//        structCopy.schema().fields().forEach(field -> {
//            final String fieldName = field.name();
//            final List<String> subPath = this.getSubPath(fieldName);
//            if (!this.isExclude(subPath)) {
//                final StructDropper deleteSchema = new StructDropper(this.excludePath, subPath);
//                Object struct1 = struct.get(fieldName);
//                Object transform = deleteSchema.transform(struct1, fieldName);
//                structCopy.put(fieldName, transform);
//            }
//        });
//    }

    private Object transform(final Object struct, final String fieldName) {
        if (struct instanceof Struct) {
            Struct struct2 = (Struct) struct;
            return this.processStruct(struct2, struct2.schema());
        }
        // } else if (struct instanceof List) {
        //     return this.processArray(struct);
        // }
        return struct;
    }

//    private Struct processArray(final Struct value) {
//        List<Struct> arrayValues = value.getArray(value.schema().name());
//
//        Struct innerStruct = new Struct(value.schema().valueSchema());
//        for (Struct arrayValue : arrayValues) {
//            addFields(arrayValue, innerStruct);
//        }
//        return innerStruct;
//    }

    private boolean isExclude(final List<String> strings) {
        return this.excludePath.equals(strings);
    }

    private List<String> getSubPath(final String fieldName) {
        final List<String> strings = new ArrayList<>(this.currentPath);
        strings.add(fieldName);
        return strings;
    }
    // private final Path path;
    // private Struct oldValue;
    // @Getter
    // private Struct updatedValue;

    // @Override
    // public Collection<Field> fields() {
    // return this.updatedValue.schema().fields();
    // }

    // @Override
    // public void onArray(final Field field) {
    // final String fieldName = field.name();
    // final Iterable<Struct> arrayValues = this.oldValue.getArray(fieldName);
    // final Collection<Struct> updatedArrayValues =
    // this.addArrayValues(this.updatedValue, field, arrayValues);
    // this.updatedValue.put(fieldName, updatedArrayValues);
    // }

    // @Override
    // public void onStruct(final Field field) {
    // final String fieldName = field.name();
    // final Struct structWithValue = this.oldValue.getStruct(fieldName);
    // final Struct updatedNestedStruct = new
    // Struct(this.updatedValue.schema().field(fieldName).schema());
    // final NestedFieldParser nestedFieldParser =
    // new DeleteStructValue(this.path, structWithValue, updatedNestedStruct);
    // nestedFieldParser.iterate(this.path);
    // this.updatedValue.put(fieldName, updatedNestedStruct);
    // }

    // @Override
    // public void onDefault(final Field field) {
    // this.updatedValue.put(field.name(), this.oldValue.get(field.name()));
    // }

    // @Override
    // public void onLastElementPath(final Field field) {
    // if (!field.name().equals(this.path.getLastElement())) {
    // this.updatedValue.put(field.name(), this.oldValue.get(field.name()));
    // }
    // }

    // private Collection<Struct> addArrayValues(final Struct updatedValue,
    // final Field field, final Iterable<? extends Struct> arrayValues) {
    // final Collection<Struct> values = new ArrayList<>();
    // for (final Struct arrayValue : arrayValues) {
    // final Struct updatedNestedStruct =
    // new Struct(updatedValue.schema().field(field.name()).schema().valueSchema());
    // final NestedFieldParser nestedFieldParser =
    // new DeleteStructValue(this.path, arrayValue, updatedNestedStruct);
    // nestedFieldParser.iterate(this.path);
    // values.add(updatedNestedStruct);
    // }
    // return values;
    // }
}
