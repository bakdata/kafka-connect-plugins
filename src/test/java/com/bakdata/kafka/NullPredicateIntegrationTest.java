/*
 * MIT License
 *
 * Copyright (c) 2025 bakdata
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

import static net.mguenther.kafka.junit.EmbeddedConnectConfig.kafkaConnect;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.newClusterConfig;
import static net.mguenther.kafka.junit.SendKeyValues.to;
import static net.mguenther.kafka.junit.TopicConfig.withName;
import static net.mguenther.kafka.junit.Wait.delay;
import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.schemaregistrymock.junit5.SchemaRegistryMockExtension;
import com.bakdata.test.smt.NullableObject;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
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
import org.apache.kafka.connect.transforms.Filter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class NullPredicateIntegrationTest {
    private static final String TOPIC = "input";
    @RegisterExtension
    final SchemaRegistryMockExtension schemaRegistryMock = new SchemaRegistryMockExtension();
    private EmbeddedKafkaCluster kafkaCluster;
    private Path outputFile;

    private static NullableObject.Builder createValue() {
        return NullableObject.newBuilder().setField1("field1");
    }

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
    void shouldDropNullField() throws InterruptedException, IOException {
        final NullableObject value1 = createValue().setField2("field2").build();
        final NullableObject value2 = createValue().build();

        final List<KeyValue<String, NullableObject>> records =
                List.of(new KeyValue<>("k1", value1), new KeyValue<>("k2", value2));
        this.kafkaCluster.send(to(TOPIC, records)
                .withAll(this.createProducerProperties())
                .build());

        // makes sure that both records are processed
        delay(2, TimeUnit.SECONDS);
        final List<String> output = Files.readAllLines(this.outputFile);
        assertThat(output).containsExactly("Struct{field1=field1,field2=field2}");
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
        properties.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, AvroConverter.class.getName());
        properties.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG + "."
                + AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistryMock.getUrl());

        properties.put(SinkConnector.TOPICS_CONFIG, TOPIC);
        properties.put(FileStreamSinkConnector.FILE_CONFIG, this.outputFile.toString());

        // SMT config
        properties.put("transforms", "Filter");
        properties.put("transforms.Filter.type", Filter.class.getName());
        properties.put("transforms.Filter.predicate", "NonNull");

        properties.put("predicates", "NonNull");
        properties.put("predicates.NonNull.type", NullPredicate.class.getName());
        properties.put("predicates.NonNull.field", "field2");
        return properties;
    }

    private Properties createProducerProperties() {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaCluster.getBrokerList());
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistryMock.getUrl());
        return properties;
    }
}
