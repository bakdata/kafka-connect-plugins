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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

// TODO: Use SoftAssertions
class FieldDropperTest {

    /**
     * Before:
     * <pre>
     *     {@code
     *          {
     *              "complex_field": {
     *                  "kept_field": 1234
     *              },
     *              "boolean_field": true
     *          }
     *     }
     * </pre>
     *
     * Exclude path: ""
     * <p>
     * After:
     * <pre>
     *     {@code
     *          {
     *              "complex_field": {
     *                  "kept_field": 1234
     *              },
     *              "boolean_field": true
     *          }
     *     }
     * </pre>
     */
    @Test
    void shouldDropNoFieldIfNoExcludeIsGiven() {
        final FieldDropper computerStruct = FieldDropper.defaultFieldDropper();
        final Schema primitiveSchema = SchemaBuilder
            .struct()
            .name("PrimitiveObject")
            .field("kept_field", Schema.INT32_SCHEMA)
            .build();

        final Struct primitiveObject = new Struct(primitiveSchema);
        primitiveObject.put("kept_field", 1234);

        final Schema complexSchema = SchemaBuilder.struct()
            .name("complexObject")
            .field("complex_field", primitiveSchema)
            .field("boolean_field", Schema.BOOLEAN_SCHEMA)
            .build();

        final Struct complexObject = new Struct(complexSchema);
        complexObject.put("complex_field", primitiveObject);
        complexObject.put("boolean_field", true);

        final Struct newStruct = computerStruct.updateStruct(complexObject);

        final Field complexField = newStruct.schema().field("complex_field");
        assertThat(complexField).isNotNull();
        assertThat(newStruct.getStruct("complex_field").getInt32("kept_field")).isEqualTo(1234);
        assertThat(newStruct.getBoolean("boolean_field")).isTrue();
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
        final FieldDropper computerStruct = FieldDropper.createFieldDropper(List.of("complex_field.dropped_field"));
        final Schema innerSchema = SchemaBuilder
            .struct()
            .name("testLog")
            .field("dropped_field", Schema.STRING_SCHEMA)
            .field("kept_field", Schema.INT32_SCHEMA);

        final Struct innerStruct = new Struct(innerSchema);
        innerStruct.put("dropped_field", "This value will be dropped.");
        innerStruct.put("kept_field", 1234);

        final Schema complexSchema = SchemaBuilder.struct()
            .name("nested_object")
            .field("complex_field", innerSchema)
            .field("boolean_field", Schema.BOOLEAN_SCHEMA);

        final Struct complexObject = new Struct(complexSchema);
        complexObject.put("complex_field", innerStruct);
        complexObject.put("boolean_field", true);

        final Struct newStruct = computerStruct.updateStruct(complexObject);

        final Struct complexField = newStruct.getStruct("complex_field");
        assertThat(complexField.schema().fields()).hasSize(1);
        assertThat(complexField.schema().field("kept_field")).isNotNull();
        assertThat(complexField.schema().field("dropped_field")).isNull();
        assertThat(complexField.getInt32("kept_field")).isEqualTo(1234);
        assertThat(newStruct.get("boolean_field")).isEqualTo(true);
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
        final FieldDropper computerStruct = FieldDropper.createFieldDropper(List.of("complex_field"));
        final Schema primitiveSchema = SchemaBuilder
            .struct()
            .name("PrimitiveObject")
            .field("dropped_field", Schema.STRING_SCHEMA)
            .build();

        final Struct primitiveObject = new Struct(primitiveSchema);
        primitiveObject.put("dropped_field", "This value will be dropped.");

        final Schema complexSchema = SchemaBuilder.struct()
            .name("complexObject")
            .field("complex_field", primitiveSchema)
            .field("boolean_field", Schema.BOOLEAN_SCHEMA)
            .build();

        final Struct complexObject = new Struct(complexSchema);
        complexObject.put("complex_field", primitiveObject);
        complexObject.put("boolean_field", true);

        final Struct newStruct = computerStruct.updateStruct(complexObject);

        final Field complexField = newStruct.schema().field("complex_field");
        assertThat(complexField).isNull();
        assertThat(newStruct.getBoolean("boolean_field")).isTrue();
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
        final FieldDropper computerStruct = FieldDropper.createFieldDropper(List.of("collections.dropped_field"));

        final Schema primitiveSchema = SchemaBuilder
            .struct()
            .name("PrimitiveObject")
            .field("dropped_field", Schema.STRING_SCHEMA)
            .field("kept_field", Schema.INT32_SCHEMA)
            .build();

        final Struct primitiveObject = new Struct(primitiveSchema);
        primitiveObject.put("dropped_field", "This field will be dropped.");
        primitiveObject.put("kept_field", 1234);

        final Struct primitiveObject2 = new Struct(primitiveSchema);
        primitiveObject2.put("dropped_field", "This field will also be dropped.");
        primitiveObject2.put("kept_field", 5678);

        final Schema collectionsSchema = SchemaBuilder
            .array(primitiveSchema)
            .name("collections")
            .defaultValue(Collections.emptyList())
            .build();
        final Schema complexSchema = SchemaBuilder
            .struct()
            .name("RecordCollection")
            .field("collections", collectionsSchema)
            .field("primitive_field", Schema.INT32_SCHEMA)
            .build();

        final List<Struct> structList = List.of(primitiveObject, primitiveObject2);
        final Struct complexObject = new Struct(complexSchema);
        complexObject.put("collections", structList);
        complexObject.put("primitive_field", 9876);

        final Struct newStruct = computerStruct.updateStruct(complexObject);

        final Field collectionField = newStruct.schema().field("collections");
        assertThat(collectionField).isNotNull();
        assertThat(collectionField.schema().valueSchema().fields()).hasSize(1);
        assertThat(newStruct.getArray("collections")).hasSize(2);
        assertThat(newStruct.getArray("collections")).first().isInstanceOfSatisfying(Struct.class, primitiveStruct -> {
            assertThat(primitiveStruct.getInt32("kept_field")).isEqualTo(1234);
            assertThat(primitiveStruct.schema().field("dropped_field")).isNull();
        });
        assertThat(newStruct.getArray("collections").get(1)).isInstanceOfSatisfying(Struct.class, primitiveStruct -> {
            assertThat(primitiveStruct.getInt32("kept_field")).isEqualTo(5678);
            assertThat(primitiveStruct.schema().field("dropped_field")).isNull();
        });
        assertThat(newStruct.get("primitive_field")).isEqualTo(9876);
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
        final FieldDropper computerStruct = FieldDropper.createFieldDropper(List.of("collections"));

        final Schema primitiveSchema = SchemaBuilder
            .struct()
            .name("PrimitiveObject")
            .field("dropped_field", Schema.STRING_SCHEMA)
            .field("kept_field", Schema.INT32_SCHEMA)
            .build();

        final Struct primitiveStruct = new Struct(primitiveSchema);
        primitiveStruct.put("dropped_field", "This field will be dropped.");
        primitiveStruct.put("kept_field", 1234);

        final Struct primitiveStruct2 = new Struct(primitiveSchema);
        primitiveStruct2.put("dropped_field", "This field will also be dropped.");
        primitiveStruct2.put("kept_field", 5678);

        final Schema collectionSchema =
            SchemaBuilder.array(primitiveSchema).name("collections").defaultValue(Collections.emptyList())
                .build();
        final Schema complexSchema = SchemaBuilder
            .struct()
            .name("RecordCollection")
            .field("collections", collectionSchema)
            .field("primitive_field", Schema.INT32_SCHEMA)
            .build();

        final List<Struct> structList = List.of(primitiveStruct, primitiveStruct2);
        final Struct complexObject = new Struct(complexSchema);
        complexObject.put("collections", structList);
        complexObject.put("primitive_field", 9876);

        final Struct newStruct = computerStruct.updateStruct(complexObject);

        assertThat(newStruct.schema().field("collections")).isNull();
        assertThat(newStruct.get("primitive_field")).isEqualTo(9876);
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
        final FieldDropper computerStruct = FieldDropper
            .createFieldDropper(List.of("collections.complex_field.dropped_field"));

        final Schema primitiveSchema = SchemaBuilder
            .struct()
            .name("primitiveFields")
            .field("dropped_field", Schema.STRING_SCHEMA)
            .field("kept_field", Schema.INT32_SCHEMA)
            .build();

        final Struct primitiveStruct = new Struct(primitiveSchema);
        primitiveStruct.put("dropped_field", "This field will be dropped.");
        primitiveStruct.put("kept_field", 1234);

        final Struct primitiveStruct2 = new Struct(primitiveSchema);
        primitiveStruct2.put("dropped_field", "This field will also be dropped.");
        primitiveStruct2.put("kept_field", 5678);

        final Schema complexSchema = SchemaBuilder.struct()
            .name("nestedObject")
            .field("complex_field", primitiveSchema)
            .field("boolean_field", Schema.BOOLEAN_SCHEMA)
            .build();

        final Struct complexObject = new Struct(complexSchema);
        complexObject.put("complex_field", primitiveStruct);
        complexObject.put("boolean_field", true);

        final Struct complexObject2 = new Struct(complexSchema);
        complexObject2.put("complex_field", primitiveStruct2);
        complexObject2.put("boolean_field", false);

        final Schema recordCollectionsSchema = SchemaBuilder
            .struct()
            .name("RecordCollection")
            .field("collections",
                SchemaBuilder.array(complexSchema).name("collections").defaultValue(Collections.emptyList())
                    .build())
            .field("primitive_field", Schema.INT32_SCHEMA)
            .build();

        final List<Struct> structList = List.of(complexObject, complexObject2);
        final Struct recordCollection = new Struct(recordCollectionsSchema);
        recordCollection.put("collections", structList);
        recordCollection.put("primitive_field", 9876);

        final Struct newStruct = computerStruct.updateStruct(recordCollection);

        final Field collectionField = newStruct.schema().field("collections");
        assertThat(collectionField).isNotNull();

        assertThat(newStruct.getArray("collections")).hasSize(2);

        assertThat(newStruct.getArray("collections")).first().isInstanceOfSatisfying(Struct.class, struct -> {
            assertThat(struct.getStruct("complex_field").getInt32("kept_field")).isEqualTo(1234);
            assertThat(struct.getStruct("complex_field").schema().field("dropped_field")).isNull();
            assertThat(struct.getBoolean("boolean_field")).isTrue();
        });

        assertThat(newStruct.getArray("collections").get(1)).isInstanceOfSatisfying(Struct.class, struct -> {
            assertThat(struct.getStruct("complex_field").getInt32("kept_field")).isEqualTo(5678);
            assertThat(struct.getStruct("complex_field").schema().field("dropped_field")).isNull();
            assertThat(struct.getBoolean("boolean_field")).isFalse();
        });

        assertThat(newStruct.getInt32("primitive_field")).isEqualTo(9876);
    }
}
