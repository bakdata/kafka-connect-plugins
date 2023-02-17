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
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class JsonFieldDropperTest {
    @InjectSoftAssertions
    private SoftAssertions softly;

    /**
     * Before:
     * <pre>
     *     {@code
     *          {
     *              "boolean_field": true,
     *              "complex_field": {
     *                  "kept_field": 1234
     *              }
     *          }
     *     }
     * </pre>
     *
     * Exclude path: boolean_field
     * <p>
     * After:
     * <pre>
     *     {@code
     *          {
     *              "complex_field": {
     *                  "kept_field": 1234
     *              },
     *          }
     *     }
     * </pre>
     */
    @Test
    void shouldDropNotNestedField() {
        final JsonFieldDropper computerStruct = JsonFieldDropper.createJsonFieldDropper("boolean_field");

        final ObjectNode primitiveObject = JsonNodeFactory.instance.objectNode();
        primitiveObject.set("kept_field", JsonNodeFactory.instance.numberNode(1234));

        final ObjectNode complexObject = JsonNodeFactory.instance.objectNode();
        complexObject.set("boolean_field", JsonNodeFactory.instance.booleanNode(true));
        complexObject.set("complex_field", primitiveObject);

        final ObjectNode newStruct = computerStruct.updateJsonNode(complexObject);

        this.softly.assertThat(newStruct.get("complex_field").get("kept_field").intValue()).isEqualTo(1234);
        this.softly.assertThat(newStruct.get("boolean_field")).isNull();

    }


    /**
     * Before:
     * <pre>
     *     {@code
     *          {
     *              "dropped_field": "This field should stay here",
     *              "complex_field": {
     *                  "dropped_field": "This field will be dropped",
     *                  "kept_field": 1234
     *              }
     *          }
     *     }
     * </pre>
     *
     * Exclude path: complex_field.dropped_field
     * <p>
     * After:
     * <pre>
     *     {@code
     *          {
     *              "dropped_field": "This field should stay here",
     *              "complex_field": {
     *                  "kept_field": 1234
     *              },
     *          }
     *     }
     * </pre>
     */
    @Test
    void shouldDropCorrectFieldIfNamesAreDuplicate() {
        final JsonFieldDropper computerStruct =
            JsonFieldDropper.createJsonFieldDropper("complex_field.dropped_field");

        final ObjectNode primitiveObject = JsonNodeFactory.instance.objectNode();
        primitiveObject.set("dropped_field", JsonNodeFactory.instance.textNode("This field will be dropped"));
        primitiveObject.set("kept_field", JsonNodeFactory.instance.numberNode(1234));

        final ObjectNode complexObject = JsonNodeFactory.instance.objectNode();
        complexObject.set("dropped_field", JsonNodeFactory.instance.textNode("This field should stay here"));
        complexObject.set("complex_field", primitiveObject);

        final ObjectNode newStruct = computerStruct.updateJsonNode(complexObject);

        final JsonNode complexField = newStruct.get("complex_field");
        this.softly.assertThat(complexField.get("dropped_field")).isNull();
        this.softly.assertThat(complexField.get("kept_field").intValue()).isEqualTo(1234);
        this.softly.assertThat(newStruct.get("dropped_field").textValue()).isEqualTo("This field should stay here");
    }

    /**
     * Before:
     * <pre>
     *     {@code
     *          {
     *              "complex_field": {
     *                  "dropped_field": "This field will be dropped."
     *                  "kept_field": 1234
     *              },
     *              "boolean_field": true
     *          }
     *     }
     * </pre>
     *
     * Exclude path: complex_field.dropped_field
     * <p>
     * After:
     * <pre>
     *     {@code
     *          {
     *             "complex_field": {
     *                  "kept_field": 1234
     *              },
     *              "boolean_field": true
     *          }
     *     }
     * </pre>
     */
    @Test
    void shouldDropNestedFieldInStruct() {
        final JsonFieldDropper computerStruct =
            JsonFieldDropper.createJsonFieldDropper("complex_field.dropped_field");

        final ObjectNode primitiveObject = JsonNodeFactory.instance.objectNode();
        primitiveObject.set("dropped_field", JsonNodeFactory.instance.textNode("This value will be dropped."));
        primitiveObject.set("kept_field", JsonNodeFactory.instance.numberNode(1234));

        final ObjectNode complexObject = JsonNodeFactory.instance.objectNode();
        complexObject.set("complex_field", primitiveObject);
        complexObject.set("boolean_field", JsonNodeFactory.instance.booleanNode(true));

        final ObjectNode newStruct = computerStruct.updateJsonNode(complexObject);

        final JsonNode complexField = newStruct.get("complex_field");

        this.softly.assertThat(complexField.size()).isEqualTo(1);
        this.softly.assertThat(complexField.get("kept_field").intValue()).isEqualTo(1234);
        this.softly.assertThat(complexField.get("dropped_field")).isNull();
        this.softly.assertThat(newStruct.get("boolean_field").booleanValue()).isTrue();
    }

    /**
     * Before:
     * <pre>
     *     {@code
     *          {
     *              "complex_field": {
     *                  "dropped_field": "This field will be dropped."
     *              },
     *              "boolean_field": true
     *          }
     *     }
     * </pre>
     *
     * Exclude path: complex_field
     * <p>
     * After:
     * <pre>
     *     {@code
     *          {
     *              "boolean_field": true
     *          }
     *     }
     * </pre>
     */
    @Test
    void shouldDropStruct() {
        final JsonFieldDropper computerStruct = JsonFieldDropper.createJsonFieldDropper("complex_field");

        final ObjectNode primitiveObject = JsonNodeFactory.instance.objectNode();
        primitiveObject.set("dropped_field", JsonNodeFactory.instance.textNode("This field will be dropped"));

        final ObjectNode complexObject = JsonNodeFactory.instance.objectNode();
        complexObject.set("complex_field", primitiveObject);
        complexObject.set("boolean_field", JsonNodeFactory.instance.booleanNode(true));

        final ObjectNode newStruct = computerStruct.updateJsonNode(complexObject);

        this.softly.assertThat(newStruct.get("complex_field")).isNull();
        this.softly.assertThat(newStruct.get("boolean_field").booleanValue()).isTrue();
    }

    /**
     * Before:
     * <pre>
     *     {@code
     *          {
     *              "collections": [
     *                {
     *                  "dropped_field": "This field will be dropped.",
     *                  "kept_field": 1234
     *                },
     *                {
     *                  "dropped_field": "This field will also be dropped.",
     *                  "kept_field": 5678
     *                }
     *              ],
     *              "primitive_field": 9876
     *          }
     *     }
     * </pre>
     *
     * Exclude path: collections.dropped_field
     * <p>
     * After:
     * <pre>
     *     {@code
     *          {
     *              "collections": [
     *                {
     *                  "kept_field": 1234
     *                },
     *                {
     *                  "kept_field": 5678
     *                }
     *              ],
     *              "primitive_field": 9876
     *          }
     *     }
     * </pre>
     */
    @Test
    void shouldDropFieldInArray() {
        final JsonFieldDropper computerStruct =
            JsonFieldDropper.createJsonFieldDropper("collections.dropped_field");

        final ObjectNode primitiveObject = JsonNodeFactory.instance.objectNode();
        primitiveObject.set("dropped_field", JsonNodeFactory.instance.textNode("This value will be dropped."));
        primitiveObject.set("kept_field", JsonNodeFactory.instance.numberNode(1234));

        final ObjectNode primitiveObject2 = JsonNodeFactory.instance.objectNode();
        primitiveObject2.set("dropped_field", JsonNodeFactory.instance.textNode("This field will also be dropped."));
        primitiveObject2.set("kept_field", JsonNodeFactory.instance.numberNode(5678));

        final ArrayNode collections = JsonNodeFactory.instance.arrayNode();
        collections.add(primitiveObject);
        collections.add(primitiveObject2);

        final ObjectNode complexObject = JsonNodeFactory.instance.objectNode();
        complexObject.set("collections", collections);
        complexObject.set("primitive_field", JsonNodeFactory.instance.numberNode(9876));

        final ObjectNode newStruct = computerStruct.updateJsonNode(complexObject);

        this.softly.assertThat(newStruct.get("collections"))
            .hasSize(2)
            .isInstanceOfSatisfying(ArrayNode.class, array -> {
                    this.softly.assertThat(array)
                        .first()
                        .isInstanceOfSatisfying(ObjectNode.class, primitiveStruct -> {
                                final int keptField = primitiveStruct.get("kept_field").intValue();
                                this.softly.assertThat(keptField).isEqualTo(1234);
                            }
                        );
                    this.softly.assertThat(array.get(1))
                        .isInstanceOfSatisfying(ObjectNode.class, primitiveStruct -> {
                                final int keptField = primitiveStruct.get("kept_field").intValue();
                                this.softly.assertThat(keptField).isEqualTo(5678);
                            }
                        );
                    this.softly.assertThat(array).allSatisfy(
                        arrayNode -> this.softly.assertThat(arrayNode)
                            .isInstanceOfSatisfying(ObjectNode.class, objectNode -> {
                                    final JsonNode droppedField = objectNode.get("dropped_field");
                                    this.softly.assertThat(droppedField).isNull();
                                }
                            )
                    );
                }
            );
        this.softly.assertThat(newStruct.get("primitive_field").intValue()).isEqualTo(9876);
    }


    /**
     * Before:
     * <pre>
     *     {@code
     *          {
     *              "collections": [
     *                {
     *                  "dropped_field": "This field will be dropped.",
     *                  "kept_field": 1234
     *                },
     *                {
     *                  "dropped_field": "This field will also be dropped.",
     *                  "kept_field": 5678
     *                }
     *              ],
     *              "primitive_field": 9876
     *          }
     *     }
     * </pre>
     *
     * Exclude path: collections
     * <p>
     * After:
     * <pre>
     *     {@code
     *          {
     *              "primitive_field": 9876
     *          }
     *     }
     * </pre>
     */
    @Test
    void shouldDropArray() {
        final JsonFieldDropper computerStruct = JsonFieldDropper.createJsonFieldDropper("collections");

        final ObjectNode primitiveObject = JsonNodeFactory.instance.objectNode();
        primitiveObject.set("dropped_field", JsonNodeFactory.instance.textNode("This value will be dropped."));
        primitiveObject.set("kept_field", JsonNodeFactory.instance.numberNode(1234));

        final ObjectNode primitiveObject2 = JsonNodeFactory.instance.objectNode();
        primitiveObject2.set("dropped_field", JsonNodeFactory.instance.textNode("This field will also be dropped."));
        primitiveObject2.set("kept_field", JsonNodeFactory.instance.numberNode(5678));

        final ArrayNode collections = JsonNodeFactory.instance.arrayNode();
        collections.add(primitiveObject);
        collections.add(primitiveObject2);

        final ObjectNode complexObject = JsonNodeFactory.instance.objectNode();
        complexObject.set("collections", collections);
        complexObject.set("primitive_field", JsonNodeFactory.instance.numberNode(9876));

        final ObjectNode newStruct = computerStruct.updateJsonNode(complexObject);

        this.softly.assertThat(newStruct.get("collections")).isNull();
        this.softly.assertThat(newStruct.get("primitive_field").intValue()).isEqualTo(9876);
    }

    /**
     * Before:
     * <pre>
     *     {@code
     *          {
     *              "collections": [
     *                {
     *                  "complex_field": {
     *                    "dropped_field": "This field will be dropped.",
     *                    "kept_field": 1234
     *                  },
     *                  "boolean_field": true
     *                },
     *                {
     *                  "complex_field": {
     *                    "dropped_field": "This field will also be dropped.",
     *                    "kept_field": 5678
     *                  },
     *                  "boolean_field": false
     *                }
     *              ],
     *              "primitive_field": 9876
     *          }
     *     }
     * </pre>
     *
     * Exclude path: collections.complex_field.dropped_field
     * <p>
     * After:
     * <pre>
     *     {@code
     *          {
     *              "collections": [
     *                {
     *                  "complex_field": {
     *                    "kept_field": 1234
     *                  },
     *                  "boolean_field": true
     *                },
     *                {
     *                  "complex_field": {
     *                    "kept_field": 5678
     *                  },
     *                  "boolean_field": false
     *                }
     *              ],
     *              "primitive_field": 9876
     *          }
     *     }
     * </pre>
     */
    @Test
    void shouldDropFieldInStructArray() {
        final JsonFieldDropper computerStruct = JsonFieldDropper
            .createJsonFieldDropper("collections.complex_field.dropped_field");

        final ObjectNode primitiveObject = JsonNodeFactory.instance.objectNode();
        primitiveObject.set("dropped_field", JsonNodeFactory.instance.textNode("This value will be dropped."));
        primitiveObject.set("kept_field", JsonNodeFactory.instance.numberNode(1234));

        final ObjectNode primitiveObject2 = JsonNodeFactory.instance.objectNode();
        primitiveObject2.set("dropped_field", JsonNodeFactory.instance.textNode("This field will also be dropped."));
        primitiveObject2.set("kept_field", JsonNodeFactory.instance.numberNode(5678));

        final ObjectNode complexObject = JsonNodeFactory.instance.objectNode();
        complexObject.set("complex_field", primitiveObject);
        complexObject.set("boolean_field", JsonNodeFactory.instance.booleanNode(true));

        final ObjectNode complexObject2 = JsonNodeFactory.instance.objectNode();
        complexObject2.set("complex_field", primitiveObject2);
        complexObject2.set("boolean_field", JsonNodeFactory.instance.booleanNode(false));

        final ArrayNode collections = JsonNodeFactory.instance.arrayNode();
        collections.add(complexObject);
        collections.add(complexObject2);

        final ObjectNode recordCollection = JsonNodeFactory.instance.objectNode();
        recordCollection.set("collections", collections);
        recordCollection.set("primitive_field", JsonNodeFactory.instance.numberNode(9876));

        final ObjectNode newStruct = computerStruct.updateJsonNode(recordCollection);

        this.softly.assertThat(newStruct.get("primitive_field").intValue()).isEqualTo(9876);
        this.softly.assertThat(newStruct.get("collections"))
            .hasSize(2)
            .isInstanceOfSatisfying(ArrayNode.class, array -> {
                    this.softly.assertThat(array)
                        .first()
                        .isInstanceOfSatisfying(ObjectNode.class, struct -> {
                                final int fieldValue = struct
                                    .get("complex_field")
                                    .get("kept_field").intValue();
                                this.softly.assertThat(fieldValue).isEqualTo(1234);
                                this.softly.assertThat(struct.get("boolean_field").booleanValue()).isTrue();
                            }
                        );
                    this.softly.assertThat(array.get(1))
                        .isInstanceOfSatisfying(ObjectNode.class, struct -> {
                                final int fieldValue = struct
                                    .get("complex_field")
                                    .get("kept_field").intValue();
                                this.softly.assertThat(fieldValue).isEqualTo(5678);
                                this.softly.assertThat(struct.get("boolean_field").booleanValue()).isFalse();
                            }
                        );
                    this.softly.assertThat(array).allSatisfy(arrayNode -> {
                        final JsonNode field = arrayNode
                            .get("complex_field")
                            .get("dropped_field");
                        this.softly.assertThat(field).isNull();
                    });
                }
            );
        this.softly.assertThat(newStruct.get("primitive_field").intValue()).isEqualTo(9876);
    }
}
