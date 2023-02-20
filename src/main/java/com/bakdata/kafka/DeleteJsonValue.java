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
import java.util.Iterator;
import java.util.Map.Entry;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.kafka.connect.errors.ConnectException;

@AllArgsConstructor
class DeleteJsonValue {
    private final Path path;
    private final JsonNode oldValue;
    @Getter
    private final JsonNode updatedValue;

    private static JsonNode getJsonNode(final JsonNode arrayValue) {
        switch (arrayValue.getNodeType()) {
            case OBJECT:
                return JsonNodeFactory.instance.objectNode();
            case ARRAY:
                return JsonNodeFactory.instance.arrayNode();
            default:
                throw new ConnectException("The input JSON is not a valid JSON.");
        }
    }

    private Iterator<Entry<String, JsonNode>> fields() {
        return this.oldValue.fields();
    }

    void iterate() {
        final int currentPathIndex = this.path.getDepth();
        for (final Iterator<Entry<String, JsonNode>> it = this.fields(); it.hasNext(); ) {
            final Entry<String, JsonNode> field = it.next();
            if (currentPathIndex == 1) {
                this.onLastElementPath(field.getKey(), field.getValue());
            } else {
                this.path.moveDeeperIntoPath();
                final JsonNode fieldValue = field.getValue();
                switch (fieldValue.getNodeType()) {
                    case ARRAY:
                        final ArrayNode arrayNode = (ArrayNode) fieldValue;
                        this.onArray(field.getKey(), arrayNode);
                        break;
                    case OBJECT:
                        final ObjectNode objectNode = (ObjectNode) fieldValue;
                        this.onStruct(field.getKey(), objectNode);
                        break;
                    default:
                        this.onDefault(field.getKey(), fieldValue);
                }
                this.path.moveHigherIntoPath();
            }
        }
    }

    private void onArray(final String fieldName, final ArrayNode arrayNode) {
        final ArrayNode updatedArrayValues = this.addArrayValues(arrayNode);
        ((ObjectNode) this.updatedValue).set(fieldName, updatedArrayValues);
    }

    private void onStruct(final String fieldName, final ObjectNode structWithValue) {
        final JsonNode updatedNestedStruct = JsonNodeFactory.instance.objectNode();
        final DeleteJsonValue deleteJsonValue = new DeleteJsonValue(this.path, structWithValue, updatedNestedStruct);
        deleteJsonValue.iterate();
        this.iterate();
        ((ObjectNode) this.updatedValue).set(fieldName, updatedNestedStruct);
    }

    private void onDefault(final String fieldName, final JsonNode field) {
        ((ObjectNode) this.updatedValue).set(fieldName, field);
    }

    private void onLastElementPath(final String fieldName, final JsonNode field) {
        if (!fieldName.equals(this.path.getLastElement())) {
            ((ObjectNode) this.updatedValue).set(fieldName, field);
        }
    }

    private ArrayNode addArrayValues(final Iterable<JsonNode> arrayValues) {
        final ArrayNode values = JsonNodeFactory.instance.arrayNode();
        for (final JsonNode arrayValue : arrayValues) {
            final JsonNode updatedNestedStruct = getJsonNode(arrayValue);
            if (arrayValue.isArray()) {
                for (final Iterator<JsonNode> it = arrayValue.elements(); it.hasNext(); ) {
                    this.path.moveDeeperIntoPath();
                    final JsonNode element = it.next();
                    final DeleteJsonValue deleteJsonValue =
                        new DeleteJsonValue(this.path, element, updatedNestedStruct);
                    deleteJsonValue.iterate();
                    this.path.moveHigherIntoPath();
                    values.add(deleteJsonValue.updatedValue);
                }
            } else {
                final DeleteJsonValue deleteJsonValue = new DeleteJsonValue(this.path, arrayValue, updatedNestedStruct);
                deleteJsonValue.iterate();
                values.add(deleteJsonValue.updatedValue);
            }
        }
        return values;
    }
}
