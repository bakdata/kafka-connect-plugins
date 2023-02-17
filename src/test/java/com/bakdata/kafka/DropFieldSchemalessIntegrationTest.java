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

import static java.util.Collections.singletonList;
import static net.mguenther.kafka.junit.EmbeddedConnectConfig.kafkaConnect;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.newClusterConfig;
import static net.mguenther.kafka.junit.SendKeyValues.to;
import static net.mguenther.kafka.junit.TopicConfig.withName;
import static net.mguenther.kafka.junit.Wait.delay;
import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.schemaregistrymock.junit5.SchemaRegistryMockExtension;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.file.FileStreamSinkConnector;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class DropFieldSchemalessIntegrationTest {
    private static final String EXCLUDE_PATH = "lastName";
    private static final String DROP_NESTED_FIELD = "DropField";
    private static final String TOPIC = "input";
    @RegisterExtension
    final SchemaRegistryMockExtension schemaRegistryMock = new SchemaRegistryMockExtension();
    private final ObjectMapper mapper = new ObjectMapper();
    private EmbeddedKafkaCluster kafkaCluster;
    private Path outputFile;

    @BeforeEach
    void setUp() throws IOException {
        this.outputFile = Files.createTempFile("test", "temp");
        this.kafkaCluster = this.createCluster();
        this.kafkaCluster.start();
        this.kafkaCluster.createTopic(withName(TOPIC).build());
    }

    @AfterEach
    void tearDown() throws IOException {
        this.kafkaCluster.stop();
        Files.deleteIfExists(this.outputFile);
    }

    @Test
    void shouldDeleteNestedField() throws InterruptedException, IOException {
        final String value = this.createValue();

        final List<KeyValue<String, Object>> records = singletonList(new KeyValue<>("k1", value));
        this.kafkaCluster.send(to(TOPIC, records)
            .withAll(this.createProducerProperties())
            .build());

        // makes sure that both records are processed
        delay(2, TimeUnit.SECONDS);
        final List<String> output = Files.readAllLines(this.outputFile);
        assertThat(output).containsExactly("{\"firstName\":\"Jack\",\"age\":25}");
    }

    private String createValue() throws JsonProcessingException {
        final User user = new User("Jack", "Java", 25);
        final JsonNode newNode = this.mapper.convertValue(user, JsonNode.class);

        return this.mapper.writeValueAsString(newNode);
    }

    private EmbeddedKafkaCluster createCluster() {
        return EmbeddedKafkaCluster.provisionWith(
            newClusterConfig()
                .configure(
                    kafkaConnect()
                        .deployConnector(this.config())
                        .build())
                .build());
    }

    private Properties config() {
        final Properties properties = new Properties();
        properties.put(ConnectorConfig.NAME_CONFIG, "test");
        properties.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, "FileStreamSink");
        properties.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        properties.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        properties.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG + "."
            + AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistryMock.getUrl());

        // SMT config
        properties.put(ConnectorConfig.TRANSFORMS_CONFIG, DROP_NESTED_FIELD);
        properties.put(ConnectorConfig.TRANSFORMS_CONFIG + "." + DROP_NESTED_FIELD + ".type",
            DropField.Value.class.getName());
        properties.put(ConnectorConfig.TRANSFORMS_CONFIG + "." + DROP_NESTED_FIELD + "." + DropField.EXCLUDE_FIELD,
            EXCLUDE_PATH);

        properties.put(SinkConnector.TOPICS_CONFIG, TOPIC);
        properties.put(FileStreamSinkConnector.FILE_CONFIG, this.outputFile.toString());
        return properties;
    }

    private Properties createProducerProperties() {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaCluster.getBrokerList());
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistryMock.getUrl());
        return properties;
    }
}