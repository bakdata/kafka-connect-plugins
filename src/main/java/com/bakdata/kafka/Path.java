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
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * An exclude object holding the logic of the exclude path.
 */
@AllArgsConstructor
@Getter
public class Path {
    private static final Pattern DOT_REGEX = Pattern.compile("\\.");
    private int depth;
    private final String lastElement;


    /**
     * A static constructor that delivers a list of exclude objects
     *
     * @param excludes List of exclude paths given by the user
     * @return List of exclude objects
     */
    public static Iterable<Path> createPathList(final Iterable<String> excludes) {
        final Collection<Path> pathPaths = new ArrayList<>();
        for (final String excludePattern : excludes) {
            final Path path = createPath(excludePattern);
            pathPaths.add(path);
        }
        return pathPaths;
    }

    /**
     * Decreases the depth of the exclude path
     */
    public void moveDeeperIntoPath() {
        this.depth--;
    }

    /**
     * Increases the depth of the exclude path
     */
    public void moveHigherIntoPath() {
        this.depth++;
    }

    private static Path createPath(final CharSequence exclude) {
        final Deque<String> nestedFields = new ArrayDeque<>(Arrays.asList(DOT_REGEX.split(exclude)));
        return new Path(nestedFields.size(), nestedFields.peekLast());
    }
}
