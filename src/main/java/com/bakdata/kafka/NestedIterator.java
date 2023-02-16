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
import org.apache.kafka.connect.data.Field;

/**
 * An interface for iterating through the given (nested) path
 */
public interface NestedIterator {
    Collection<Field> fields();
    void onArray(final Field field);
    void onStruct(final Field field);
    void onDefault(final Field field);
    void onLastElementPath(final Field field);

    default void iterate(final Exclude exclude) {
        final int currentPathIndex = exclude.getDepth();
        for (final Field field : this.fields()) {
            if (currentPathIndex == 1) {
                this.onLastElementPath(field);
            } else {
                exclude.moveDeeperIntoPath();
                switch (field.schema().type()) {
                    case ARRAY:
                        this.onArray(field);
                        break;
                    case STRUCT:
                        this.onStruct(field);
                        break;
                    default:
                        this.onDefault(field);
                }
                exclude.moveHigherIntoPath();
            }
        }
    }
}
