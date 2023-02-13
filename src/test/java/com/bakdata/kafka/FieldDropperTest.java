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

import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class FieldDropperTest {
    @InjectSoftAssertions
    private SoftAssertions softly;

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
        this.softly.assertThat(complexField).isNotNull();
        this.softly.assertThat(newStruct.getStruct("complex_field").getInt32("kept_field")).isEqualTo(1234);
        this.softly.assertThat(newStruct.getBoolean("boolean_field")).isTrue();
    }

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
        final FieldDropper computerStruct = FieldDropper.createFieldDropper(List.of("boolean_field"));
        final Schema primitiveSchema = SchemaBuilder
            .struct()
            .name("PrimitiveObject")
            .field("kept_field", Schema.INT32_SCHEMA)
            .build();

        final Struct primitiveObject = new Struct(primitiveSchema);
        primitiveObject.put("kept_field", 1234);

        final Schema complexSchema = SchemaBuilder.struct()
            .name("complexObject")
            .field("boolean_field", Schema.BOOLEAN_SCHEMA)
            .field("complex_field", primitiveSchema)
            .build();

        final Struct complexObject = new Struct(complexSchema);
        complexObject.put("boolean_field", true);
        complexObject.put("complex_field", primitiveObject);

        final Struct newStruct = computerStruct.updateStruct(complexObject);

        final Field complexField = newStruct.schema().field("complex_field");
        this.softly.assertThat(complexField).isNotNull();
        this.softly.assertThat(newStruct.getStruct("complex_field").getInt32("kept_field")).isEqualTo(1234);
        this.softly.assertThat(newStruct.schema().field("boolean_field")).isNull();

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
        final FieldDropper computerStruct = FieldDropper.createFieldDropper(List.of("complex_field.dropped_field"));
        final Schema primitiveSchema = SchemaBuilder
            .struct()
            .name("PrimitiveObject")
            .field("dropped_field", Schema.STRING_SCHEMA)
            .field("kept_field", Schema.INT32_SCHEMA)
            .build();

        final Struct primitiveObject = new Struct(primitiveSchema);
        primitiveObject.put("dropped_field", "This field will be dropped");
        primitiveObject.put("kept_field", 1234);

        final Schema complexSchema = SchemaBuilder.struct()
            .name("complexObject")
            .field("dropped_field", Schema.STRING_SCHEMA)
            .field("complex_field", primitiveSchema)
            .build();

        final Struct complexObject = new Struct(complexSchema);
        complexObject.put("dropped_field", "This field should stay here");
        complexObject.put("complex_field", primitiveObject);

        final Struct newStruct = computerStruct.updateStruct(complexObject);

        final Field complexField = newStruct.schema().field("complex_field");
        this.softly.assertThat(complexField).isNotNull();
        this.softly.assertThat(complexField.schema().field("dropped_field")).isNull();
        this.softly.assertThat(newStruct.getStruct("complex_field").getInt32("kept_field")).isEqualTo(1234);
        this.softly.assertThat(newStruct.schema().field("dropped_field")).isNotNull();
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
            .name("PrimitiveObject")
            .field("dropped_field", Schema.STRING_SCHEMA)
            .field("kept_field", Schema.INT32_SCHEMA)
            .build();

        final Struct innerStruct = new Struct(innerSchema);
        innerStruct.put("dropped_field", "This value will be dropped.");
        innerStruct.put("kept_field", 1234);

        final Schema complexSchema = SchemaBuilder.struct()
            .name("NestedObject")
            .field("complex_field", innerSchema)
            .field("boolean_field", Schema.BOOLEAN_SCHEMA)
            .build();

        final Struct complexObject = new Struct(complexSchema);
        complexObject.put("complex_field", innerStruct);
        complexObject.put("boolean_field", true);

        final Struct newStruct = computerStruct.updateStruct(complexObject);

        final Struct complexField = newStruct.getStruct("complex_field");
        this.softly.assertThat(complexField.schema().fields()).hasSize(1);
        this.softly.assertThat(complexField.schema().field("kept_field")).isNotNull();
        this.softly.assertThat(complexField.schema().field("dropped_field")).isNull();
        this.softly.assertThat(complexField.getInt32("kept_field")).isEqualTo(1234);
        this.softly.assertThat(newStruct.get("boolean_field")).isEqualTo(true);
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
        this.softly.assertThat(complexField).isNull();
        this.softly.assertThat(newStruct.getBoolean("boolean_field")).isTrue();
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
        this.softly.assertThat(collectionField).isNotNull();
        this.softly.assertThat(collectionField.schema().valueSchema().fields()).hasSize(1);
        this.softly.assertThat(newStruct.getArray("collections")).hasSize(2).satisfies(array -> {
            this.softly.assertThat(array).first().isInstanceOfSatisfying(Struct.class, primitiveStruct -> {
                this.softly.assertThat(primitiveStruct.getInt32("kept_field")).isEqualTo(1234);
                this.softly.assertThat(primitiveStruct.schema().field("dropped_field")).isNull();
            });
            this.softly.assertThat(array.get(1)).isInstanceOfSatisfying(Struct.class, primitiveStruct -> {
                this.softly.assertThat(primitiveStruct.getInt32("kept_field")).isEqualTo(5678);
                this.softly.assertThat(primitiveStruct.schema().field("dropped_field")).isNull();
            });
        });
        this.softly.assertThat(newStruct.get("primitive_field")).isEqualTo(9876);
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

        this.softly.assertThat(newStruct.schema().field("collections")).isNull();
        this.softly.assertThat(newStruct.get("primitive_field")).isEqualTo(9876);
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

        this.softly.assertThat(newStruct.getInt32("primitive_field")).isEqualTo(9876);
        this.softly.assertThat(collectionField).isNotNull();
        this.softly.assertThat(newStruct.getArray("collections")).hasSize(2).satisfies(array -> {
            this.softly.assertThat(array).first().isInstanceOfSatisfying(Struct.class, struct -> {
                this.softly.assertThat(struct.getStruct("complex_field").getInt32("kept_field")).isEqualTo(1234);
                this.softly.assertThat(struct.getStruct("complex_field").schema().field("dropped_field")).isNull();
                this.softly.assertThat(struct.getBoolean("boolean_field")).isTrue();
            });
            this.softly.assertThat(array.get(1)).isInstanceOfSatisfying(Struct.class, struct -> {
                this.softly.assertThat(struct.getStruct("complex_field").getInt32("kept_field")).isEqualTo(5678);
                this.softly.assertThat(struct.getStruct("complex_field").schema().field("dropped_field")).isNull();
                this.softly.assertThat(struct.getBoolean("boolean_field")).isFalse();
            });
        });
        this.softly.assertThat(newStruct.get("primitive_field")).isEqualTo(9876);
    }
}
