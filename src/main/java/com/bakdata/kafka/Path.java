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
import java.util.List;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class Path {
    private static final Pattern DOT_REGEX = Pattern.compile("\\.");
    private final List<String> splitPath;

    static Path split(final CharSequence exclude) {
        return new Path(Arrays.asList(DOT_REGEX.split(exclude)));
    }

    Path getSubPath(final String fieldName) {
        final List<String> strings = new ArrayList<>(this.splitPath);
        strings.add(fieldName);
        return new Path(strings);
    }

    boolean isIncluded(final Path otherPath) {
        return !otherPath.splitPath.equals(this.splitPath);
    }

    boolean isPrefix(final Path otherPath) {
        return this.splitPath.size() <= otherPath.splitPath.size() &&
            otherPath.splitPath.subList(0, this.splitPath.size()).equals(this.splitPath);
    }
}
