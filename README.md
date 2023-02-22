[![Build Status](https://dev.azure.com/bakdata/public/_apis/build/status/bakdata.kafka-connect-plugins?branchName=main)](https://dev.azure.com/bakdata/public/_build/latest?definitionId=35&branchName=main)
[![Sonarcloud status](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.kafka%3Akafka-connect-plugins&metric=alert_status)](https://sonarcloud.io/project/overview?id=com.bakdata.kafka:kafka-connect-plugins)
[![Code coverage](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.kafka%3Akafka-connect-plugins&metric=coverage)](https://sonarcloud.io/project/overview?id=com.bakdata.kafka:kafka-connect-plugins)
[![Maven](https://img.shields.io/maven-central/v/com.bakdata.kafka-connect-plugins/kafka-connect-plugins.svg)](https://search.maven.org/search?q=g:com.bakdata.kafka-connect-plugins%20AND%20a:kafka-connect-plugins&core=gav)

# Kafka Connect plugins

A collection of Kafka Connect plugins.

## Single Message Transforms (SMTs)

### Kafka Connect convert SMT

#### Description

Converts a byte record to the given converter class.

Use the concrete transformation type designed for the record key (`com.bakdata.kafka.Convert$Key`)
or value (`com.bakdata.kafka.Convert$Value`).

#### Example 

This configuration snippet shows how to use `Converter`.
It converts the value to a string schema.

```yaml
"transforms": "Convert",
"transforms.Convert.type": "com.bakdata.kafka.Convert$Value",
"transforms.Convert.converter": "org.apache.kafka.connect.storage.StringConverter"
```

#### Properties

| Name        | Description                  | Type  | Default             | Valid Values                                                                                                                                    | Importance |
|-------------|------------------------------|-------|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|------------|
| `converter` | Converter to apply to input. | class | ByteConverter.class | All classes that implement the [Kafka Converter interface](https://kafka.apache.org/24/javadoc/org/apache/kafka/connect/storage/Converter.html) | high       |

### Kafka Connect drop field(s) SMT

#### Description

Drop any (nested) field for a given path.

Use the concrete transformation type designed for the record key (`com.bakdata.kafka.DropField$Key`)
or value (`com.bakdata.kafka.DropField$Value`).



#### Example

This example shows how to configure and use `DropField`.

Imagine you have the following record value:

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
"transforms.DropField.type": "com.bakdata.kafka.DropField$Value",
"transforms.DropField.exclude": "collections.complex_field.dropped_field"
```
**Note:** If your data has bytes array schema, you can first use the `Convert` SMT to convert
your record to string. Your config would look like this:

```yaml
"transforms": "Convert,DropField",
"transforms.Convert.type": "com.bakdata.kafka.Convert$Value",
"transforms.Convert.converter": "org.apache.kafka.connect.storage.StringConverter"
"transforms.DropField.type": "com.bakdata.kafka.DropField$Value",
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

#### Properties

| Name      | Description                              | Type   | Default | Valid Values                                              | Importance |
|-----------|------------------------------------------|--------|---------|-----------------------------------------------------------|------------|
| `exclued` | Field to path from the resulting Struct. | string | -       | The path is separated by "." character. Example: `a.b.c`. | high       |

## Installation

You can install the SMT by adding the JAR file to your Kafka Connect image. For example:

```dockerfile
FROM confluentinc/cp-kafka-connect:latest

# Install your source/sink connector(s)
# ...

ENV CONNECT_PLUGIN_PATH="/connect-plugins,/usr/share/java"

# Clone the repo and build the project first. 
# Or download the JAR file from Sonatype.
COPY ./build/libs/*.jar /connect-plugins/kafka-connect-transformations/
```

## Development

If you want to contribute to this project, you can simply clone the repository and build it via Gradle.
All dependencies should be included in the Gradle files, there are no external prerequisites.

```bash
clone >git git@github.com:bakdata/kafka-connect-plugins.git
kafka-connect-plugins >cd && ./gradlew build

```

Please note, that we have [code styles](https://github.com/bakdata/bakdata-code-styles) for Java.
They are basically the Google style guide, with some small modifications.

## Contributing

We are happy if you want to contribute to this project.
If you find any bugs or have suggestions for improvements, please open an issue.
We are also happy to accept your PRs.
Just open an issue beforehand and let us know what you want to do and why.

## License

This project is licensed under the MIT license.
Have a look at the [LICENSE](https://github.com/bakdata/kafka-connect-plugins/blob/master/LICENSE) for more details.
