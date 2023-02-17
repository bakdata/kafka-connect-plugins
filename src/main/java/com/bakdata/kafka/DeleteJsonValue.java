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
import org.apache.kafka.connect.data.Struct;

/**
 * Contains logic for deleting a value
 */
@AllArgsConstructor
public class DeleteJsonValue {
    private final Path path;
    private JsonNode oldValue;

    @Getter
    private JsonNode updatedValue;

    public Iterator<Entry<String, JsonNode>> fields() {
        return this.oldValue.fields();
    }

    public void iterate(final Path path) {
        final int currentPathIndex = path.getDepth();
        for (final Iterator<Entry<String, JsonNode>> it = this.fields(); it.hasNext(); ) {
            final Entry<String, JsonNode> field = it.next();
            if (currentPathIndex == 1) {
                this.onLastElementPath(field.getKey(), field.getValue());
            } else {
                path.moveDeeperIntoPath();
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
                path.moveHigherIntoPath();
            }
        }
    }

    public void onArray(final String fieldName, final ArrayNode arrayNode) {
        final ArrayNode updatedArrayValues =
            this.addArrayValues(this.updatedValue, fieldName, arrayNode, this.path);
        ((ObjectNode)this.updatedValue).set(fieldName, updatedArrayValues);
    }

    public void onStruct(final String fieldName, final ObjectNode objectNode) {
        final JsonNode structWithValue = this.oldValue.get(fieldName);
        final JsonNode updatedNestedStruct = JsonNodeFactory.instance.objectNode();
        final JsonNode oldUpperStruct = this.oldValue;
        this.oldValue = structWithValue;
        final JsonNode upperStruct = this.updatedValue;
        this.updatedValue = updatedNestedStruct;
        this.iterate(this.path);
        this.oldValue = oldUpperStruct;
        this.updatedValue = upperStruct;
        ((ObjectNode)this.updatedValue).set(fieldName, updatedNestedStruct);
    }

    public void onDefault(final String fieldName, JsonNode field) {
        ((ObjectNode)this.updatedValue).set(fieldName, this.oldValue.get(fieldName));
    }

    public void onLastElementPath(final String fieldName, final JsonNode field) {
        if (!fieldName.equals(this.path.getLastElement())) {
            ((ObjectNode)this.updatedValue).set(fieldName, this.oldValue.get(fieldName));
        }
    }

    private ArrayNode addArrayValues(final JsonNode updatedValue,
        final String fieldName, final Iterable<JsonNode> arrayValues, final Path path) {
        final ArrayNode values = JsonNodeFactory.instance.arrayNode();
        for (final JsonNode arrayValue : arrayValues) {
            final JsonNode updatedNestedStruct = getJsonNode(arrayValue);
            final JsonNode upperOldValue = this.oldValue;
            this.oldValue = arrayValue;
            final JsonNode upperUpdatedValue = this.updatedValue;
            this.updatedValue = updatedNestedStruct;
            this.iterate(path);
            this.oldValue = upperOldValue;
            this.updatedValue = upperUpdatedValue;
            values.add(updatedNestedStruct);
        }
        return values;
    }

    private static JsonNode getJsonNode(JsonNode arrayValue) {
        switch (arrayValue.getNodeType()){
            case OBJECT:
                return JsonNodeFactory.instance.objectNode();
            default:
                throw new RuntimeException(String.format("Unsupported type %s", arrayValue.getNodeType()));
        }
    }
}
