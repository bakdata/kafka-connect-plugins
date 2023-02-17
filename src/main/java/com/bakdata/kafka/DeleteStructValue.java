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
import java.util.Collection;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

/**
 * Contains logic for deleting a value
 */
@AllArgsConstructor
public class DeleteStructValue implements NestedFieldParser {
    private final Path path;
    private Struct oldValue;
    @Getter
    private Struct updatedValue;

    @Override
    public Collection<Field> fields() {
        return this.updatedValue.schema().fields();
    }

    @Override
    public void onArray(final Field field) {
        final String fieldName = field.name();
        final Iterable<Struct> arrayValues = this.oldValue.getArray(fieldName);
        final Collection<Struct> updatedArrayValues =
            this.addArrayValues(this.updatedValue, field, arrayValues, this.path);
        this.updatedValue.put(fieldName, updatedArrayValues);
    }

    @Override
    public void onStruct(final Field field) {
        final String fieldName = field.name();
        final Struct structWithValue = this.oldValue.getStruct(fieldName);
        final Struct updatedNestedStruct = new Struct(this.updatedValue.schema().field(fieldName).schema());
        final Struct oldUpperStruct = this.oldValue;
        this.oldValue = structWithValue;
        final Struct upperStruct = this.updatedValue;
        this.updatedValue = updatedNestedStruct;
        this.iterate(this.path);
        this.oldValue = oldUpperStruct;
        this.updatedValue = upperStruct;
        this.updatedValue.put(fieldName, updatedNestedStruct);
    }

    @Override
    public void onDefault(final Field field) {
        this.updatedValue.put(field.name(), this.oldValue.get(field.name()));
    }

    @Override
    public void onLastElementPath(final Field field) {
        if (!field.name().equals(this.path.getLastElement())) {
            this.updatedValue.put(field.name(), this.oldValue.get(field.name()));
        }
    }

    private Collection<Struct> addArrayValues(final Struct updatedValue,
        final Field field, final Iterable<? extends Struct> arrayValues, final Path path) {
        final Collection<Struct> values = new ArrayList<>();
        for (final Struct arrayValue : arrayValues) {
            final Struct updatedNestedStruct =
                new Struct(updatedValue.schema().field(field.name()).schema().valueSchema());
            final Struct upperOldValue = this.oldValue;
            this.oldValue = arrayValue;
            final Struct upperUpdatedValue = this.updatedValue;
            this.updatedValue = updatedNestedStruct;
            this.iterate(path);
            this.oldValue = upperOldValue;
            this.updatedValue = upperUpdatedValue;
            values.add(updatedNestedStruct);
        }
        return values;
    }
}
