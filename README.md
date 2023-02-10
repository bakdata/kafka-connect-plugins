# Kafka Connect drop field(s) SMT

The following provides usage information for the Kafka Connect `DropField` SMT.

## Description

Drop any (nested) field for a given path.

Use the concrete transformation type designed for the record key (`org.apache.kafka.connect.transforms.DropField$Key`)
or value (`org.apache.kafka.connect.transforms.DropField$Value`).

## Installation

You can install this SMT by adding the JAR file to your Kafka Connect image. For example:

```dockerfile
FROM confluentinc/cp-kafka-connect:latest

# Install your source/sink connector(s)
# ...

ENV CONNECT_PLUGIN_PATH="/connect-plugins,/usr/share/java"

# Clone the repo and build the project first. 
COPY ./build/libs/kafka-connect-drop-field-*.jar /connect-plugins/kafka-connect-transformations/
```

## Examples

These examples show how to configure and use `DropField`.

Imagine you have the following value:

```json
{
  "collections": [
    {
      "complex_field": {
        "dropped_field": "This field will be dropped.",
        "kept_field": 1234
      },
      "boolean_field": true
    },
    {
      "complex_field": {
        "dropped_field": "This field will also be dropped.",
        "kept_field": 5678
      },
      "boolean_field": false
    }
  ],
  "primitive_field": 9876
}
```

This configuration snippet shows how to use `DropField` to exclude the field `dropped_field`.

```yaml
"transforms": "DropField",
"transforms.DropField.type": "org.apache.kafka.connect.transforms.DropField$Value",
"transforms.DropField.exclude": "collections.complex_field.dropped_field"
```

The value would transform into this:

```json
{
  "collections": [
    {
      "complex_field": {
        "kept_field": 1234
      },
      "boolean_field": true
    },
    {
      "complex_field": {
        "kept_field": 5678
      },
      "boolean_field": false
    }
  ],
  "primitive_field": 9876
}
```

## Properties

| Name      | Description                                  | Type | Default    | Valid Values                                                                                     | Importance |
|-----------|----------------------------------------------|------|------------|--------------------------------------------------------------------------------------------------|------------|
| `exclued` | Fields to exclude from the resulting Struct. | list | empty list | Comma separated strings.<br/><br/> The path is separated by "." character. Example: `a.b.c,d.e`. | medium     |
