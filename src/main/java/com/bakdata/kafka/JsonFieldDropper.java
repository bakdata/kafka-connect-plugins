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

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.cache.SynchronizedCache;

@RequiredArgsConstructor
public class JsonFieldDropper {
    private final Path excludePath;

    /**
     * Creates a  with a given list of exclude strings.
     *
     * @param exclude a list of strings to be dropped in the value
     * @return an instance of the  class with a {@link SynchronizedCache} of size 16.
     */
    public static JsonFieldDropper createJsonFieldDropper(final String exclude) {
        final Path excludePath = createPath(exclude);
        return new JsonFieldDropper(excludePath);
    }

    /**
     * This method creates the updated schema and then inserts the values based on the give exclude paths.
     *
     * @param value Old value
     * @return Updated value with the excluded field(s)
     */
    public ObjectNode updateJsonNode(final ObjectNode value) {
        final ObjectNode updatedValue = JsonNodeFactory.instance.objectNode();
        final DeleteJsonValue deleteValue = new DeleteJsonValue(this.excludePath, value, updatedValue);
        deleteValue.iterate();
        return (ObjectNode) Objects.requireNonNull(deleteValue).getUpdatedValue();
    }
}
