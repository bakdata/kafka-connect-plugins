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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.umd.cs.findbugs.annotations.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class JsonFieldDropper {
    @NonNull
    private final PathTraverser pathTraverser;

    static JsonFieldDropper createJsonFieldDropper(final Path excludePath) {
        return new JsonFieldDropper(PathTraverser.initialize(excludePath));
    }

    ObjectNode processObject(final ObjectNode value) {
        final ObjectNode objectCopy = JsonNodeFactory.instance.objectNode();
        value.fields().forEachRemaining(field -> {
                final String fieldName = field.getKey();
                final PathTraverser subPath = this.pathTraverser.getSubPath(fieldName);
                if (subPath.isIncluded()) {
                    JsonNode fieldValue = field.getValue();
                    if (subPath.isPrefix()) {
                        final JsonFieldDropper jsonFieldDropper = new JsonFieldDropper(subPath);
                        fieldValue = jsonFieldDropper.transform(fieldValue);
                    }
                    objectCopy.set(fieldName, fieldValue);
                }
            }
        );
        return objectCopy;
    }

    private JsonNode transform(final JsonNode value) {
        switch (value.getNodeType()) {
            case ARRAY:
                return this.processArray(value);
            case OBJECT:
                return this.processObject((ObjectNode) value);
            default:
                return value;
        }
    }

    private ArrayNode processArray(final Iterable<? extends JsonNode> value) {
        final ArrayNode arrayCopy = JsonNodeFactory.instance.arrayNode();
        for (final JsonNode jsonNode : value) {
            arrayCopy.add(this.transform(jsonNode));
        }
        return arrayCopy;
    }
}
