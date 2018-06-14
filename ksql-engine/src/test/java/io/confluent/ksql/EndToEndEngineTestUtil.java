/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.ksql;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.util.KsqlConstants;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.apache.kafka.test.TestUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.util.FakeKafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryMetadata;

class EndToEndEngineTestUtil {
  protected interface SerdeSupplier<T> {
    Serializer<T> getSerializer(SchemaRegistryClient schemaRegistryClient);
    Deserializer<T> getDeserializer(SchemaRegistryClient schemaRegistryClient);
  }

  static class StringSerdeSupplier implements SerdeSupplier<String> {
    @Override
    public Serializer<String> getSerializer(final SchemaRegistryClient schemaRegistryClient) {
      return Serdes.String().serializer();
    }

    @Override
    public Deserializer<String> getDeserializer(final SchemaRegistryClient schemaRegistryClient) {
      return Serdes.String().deserializer();
    }
  }

  static class AvroSerdeSupplier implements SerdeSupplier {
    @Override
    public Serializer getSerializer(final SchemaRegistryClient schemaRegistryClient) {
      return new KafkaAvroSerializer(schemaRegistryClient);
    }

    @Override
    public Deserializer getDeserializer(final SchemaRegistryClient schemaRegistryClient) {
      return new KafkaAvroDeserializer(schemaRegistryClient);
    }
  }

  static class ValueSpecAvroDeserializer implements Deserializer<Object> {
    private final SchemaRegistryClient schemaRegistryClient;
    private final KafkaAvroDeserializer avroDeserializer;

    public ValueSpecAvroDeserializer(final SchemaRegistryClient schemaRegistryClient) {
      this.schemaRegistryClient = schemaRegistryClient;
      this.avroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(final Map<String, ?> properties, final boolean b) {
    }

    @Override
    public Object deserialize(final String topicName, final byte[] data) {
      Object avroObject = avroDeserializer.deserialize(topicName, data);
      String schemaString;
      try {
        schemaString = schemaRegistryClient.getLatestSchemaMetadata(topicName + "-value").getSchema();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return avroToValueSpec(
          avroObject,
          new org.apache.avro.Schema.Parser().parse(schemaString),
          false);
    }
  }

  static class ValueSpecAvroSerializer implements Serializer<Object> {
    private final SchemaRegistryClient schemaRegistryClient;
    private final KafkaAvroSerializer avroSerializer;

    public ValueSpecAvroSerializer(SchemaRegistryClient schemaRegistryClient) {
      this.schemaRegistryClient = schemaRegistryClient;
      this.avroSerializer = new KafkaAvroSerializer(schemaRegistryClient);
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(final Map<String, ?> properties, final boolean b) {
    }

    @Override
    public byte[] serialize(final String topicName, final Object spec) {
      final String schemaString;
      try {
        schemaString = schemaRegistryClient.getLatestSchemaMetadata(topicName + "-value").getSchema();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      final Object avroObject = valueSpecToAvro(
          spec,
          new org.apache.avro.Schema.Parser().parse(schemaString));
      return avroSerializer.serialize(topicName, avroObject);
    }
  }

  static class ValueSpecAvroSerdeSupplier implements SerdeSupplier<Object> {
    @Override
    public Serializer<Object> getSerializer(final SchemaRegistryClient schemaRegistryClient) {
      return new ValueSpecAvroSerializer(schemaRegistryClient);
    }

    @Override
    public Deserializer<Object> getDeserializer(final SchemaRegistryClient schemaRegistryClient) {
      return new ValueSpecAvroDeserializer(schemaRegistryClient);
    }
  }

  static class ValueSpecJsonDeserializer implements Deserializer<Object> {
    @Override
    public void close() {
    }

    @Override
    public void configure(final Map<String, ?> properties, final boolean b) {
    }

    @Override
    public Object deserialize(final String topicName, final byte[] data) {
      try {
        return new ObjectMapper().readValue(data, Map.class);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  static class ValueSpecJsonSerializer implements Serializer<Object> {
    @Override
    public void close() {
    }

    @Override
    public void configure(final Map<String, ?> properties, final boolean b) {
    }

    @Override
    public byte[] serialize(final String topicName, final Object spec) {
      try {
        return new ObjectMapper().writeValueAsBytes(spec);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  static class ValueSpecJsonSerdeSupplier implements SerdeSupplier<Object> {
    @Override
    public Serializer<Object> getSerializer(final SchemaRegistryClient schemaRegistryClient) {
      return new ValueSpecJsonSerializer();
    }

    @Override
    public Deserializer<Object> getDeserializer(final SchemaRegistryClient schemaRegistryClient) {
      return new ValueSpecJsonDeserializer();
    }
  }

  static class Topic {
    private final String name;
    private final org.apache.avro.Schema schema;
    private final SerdeSupplier serdeSupplier;

    Topic(final String name, final org.apache.avro.Schema schema,
          final SerdeSupplier serdeSupplier) {
      this.name = name;
      this.schema = schema;
      this.serdeSupplier = serdeSupplier;
    }

    public String getName() {
      return name;
    }

    public org.apache.avro.Schema getSchema() {
      return schema;
    }

    public SerdeSupplier getSerdeSupplier() {
      return serdeSupplier;
    }

    public Serializer getSerializer(SchemaRegistryClient schemaRegistryClient) {
      return serdeSupplier.getSerializer(schemaRegistryClient);
    }

    public Deserializer getDeserializer(SchemaRegistryClient schemaRegistryClient) {
      return serdeSupplier.getDeserializer(schemaRegistryClient);
    }
  }

  static class Window {
    private final long start;
    private final long end;

    public Window(final long start, final long end) {
      this.start = start;
      this.end = end;
    }

    public long size() {
      return end - start;
    }
  }

  static class Record {
    private final Topic topic;
    private final String key;
    private final Object value;
    private final long timestamp;
    private final Window window;

    Record(final Topic topic,
           final String key,
           final Object value,
           final long timestamp,
           final Window window) {
      this.topic = topic;
      this.key = key;
      this.value = value;
      this.timestamp = timestamp;
      this.window = window;
    }

    @SuppressWarnings("unchecked")
    public Deserializer keyDeserializer() {
      if (window == null) {
        return Serdes.String().deserializer();
      }
      return new TimeWindowedDeserializer(Serdes.String().deserializer(), window.size());
    }

    @SuppressWarnings("unchecked")
    public <W> W key() {
      if (window == null) {
        return (W) key;
      }
      return (W) new Windowed<>(key, new TimeWindow(window.start, window.end));
    }

    public Object value() {
      return value;
    }

    public Window window() {
      return window;
    }

    public long timestamp() {
      return timestamp;
    }
  }

  static class Query {
    private final String testPath;
    private final String name;
    private final Collection<Topic> topics;
    private final List<Record> inputRecords;
    private final List<Record> outputRecords;
    private final List<String> statements;

    public String getName() {
      return name;
    }

    Query(
        final String testPath,
        final String name,
        final List<Topic> topics,
        final List<Record> inputRecords,
        final List<Record> outputRecords,
        final List<String> statements) {
      this.topics = topics;
      this.inputRecords = inputRecords;
      this.outputRecords = outputRecords;
      this.testPath = testPath;
      this.name = name;
      this.statements = statements;
    }

    public List<String> statements() {
      return statements;
    }

    @SuppressWarnings("unchecked")
    void processInput(final TopologyTestDriver testDriver,
                      final SchemaRegistryClient schemaRegistryClient) {
      inputRecords.forEach(
          r -> testDriver.pipeInput(
              new ConsumerRecordFactory<>(
                  Serdes.String().serializer(),
                  r.topic.getSerializer(schemaRegistryClient)
              ).create(r.topic.name, r.key, r.value, r.timestamp)
          )
      );
    }

    @SuppressWarnings("unchecked")
    void verifyOutput(final TopologyTestDriver testDriver,
                      final SchemaRegistryClient schemaRegistryClient) {
      try {
        outputRecords.forEach(
            r -> OutputVerifier.compareKeyValueTimestamp(
                testDriver.readOutput(
                    r.topic.name, r.keyDeserializer(),
                    r.topic.getDeserializer(schemaRegistryClient)
                ),
                r.key(),
                r.value,
                r.timestamp)
        );
      } catch (AssertionError assertionError) {
        throw new AssertionError("Query name: "
            + name
            + " in file: " + testPath
            + " failed due to: "
            + assertionError.getMessage());
      }
    }

    void initializeTopics(final KsqlEngine ksqlEngine) {
      for (Topic topic : topics) {
        ksqlEngine.getTopicClient().createTopic(topic.getName(), 1, (short) 1);
        if (topic.getSchema() != null) {
          try {
            ksqlEngine.getSchemaRegistryClient().register(
                topic.getName() + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX, topic.getSchema());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
  }

  private static TopologyTestDriver buildStreamsTopology(final Query query,
                                                         final KsqlEngine ksqlEngine,
                                                         final Properties streamsProperties) {
    final List<QueryMetadata> queries = new ArrayList<>();
    query.statements().forEach(
        q -> queries.addAll(ksqlEngine.buildMultipleQueries(q, Collections.emptyMap()))
    );
    return new TopologyTestDriver(queries.get(queries.size() - 1).getTopology(),
        streamsProperties,
        0);
  }

  static List<String> findTests(final String dir) throws IOException {
    final List<String> tests = new ArrayList<>();
    try (final BufferedReader reader =
             new BufferedReader(
                 new InputStreamReader(EndToEndEngineTestUtil.class.getClassLoader().
                     getResourceAsStream(dir)))) {

      String test;
      while ((test = reader.readLine()) != null) {
        if (test.endsWith(".json")) {
          tests.add(test);
        }
      }
    }
    return tests;
  }

  static void shouldBuildAndExecuteQuery(final Query query) {
    final MetaStore metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    final Map<String, Object> config = new HashMap<String, Object>() {{
      put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:0");
      put("application.id", "KSQL-TEST");
      put("commit.interval.ms", 0);
      put("cache.max.bytes.buffering", 0);
      put("auto.offset.reset", "earliest");
      put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
    }};
    final Properties streamsProperties = new Properties();
    final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();

    streamsProperties.putAll(config);

    try (final KsqlEngine ksqlEngine = new KsqlEngine(
        new KsqlConfig(config),
        new FakeKafkaTopicClient(),
        schemaRegistryClient,
        metaStore
    )) {
      query.initializeTopics(ksqlEngine);
      final TopologyTestDriver testDriver = buildStreamsTopology(query, ksqlEngine, streamsProperties);
      query.processInput(testDriver, schemaRegistryClient);
      query.verifyOutput(testDriver, schemaRegistryClient);
    }
  }

  @SuppressWarnings("unchecked")
  static Object valueSpecToAvro(final Object spec, final org.apache.avro.Schema schema) {
    if (spec == null) {
      return null;
    }
    switch (schema.getType()) {
      case INT:
      case LONG:
      case STRING:
      case DOUBLE:
      case FLOAT:
      case BOOLEAN:
        return spec;
      case ARRAY:
        return ((List)spec).stream()
            .map(o -> valueSpecToAvro(o, schema.getElementType()))
            .collect(Collectors.toList());
      case MAP:
        return ((Map<Object, Object>)spec).entrySet().stream().collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e -> valueSpecToAvro(e.getValue(), schema.getValueType())
            )
        );
      case RECORD:
        final GenericRecord record = new GenericData.Record(schema);
        for (org.apache.avro.Schema.Field field : schema.getFields()) {
          record.put(field.name(), ((Map<String, ?>)spec).get(field.name()));
        }
        return record;
      case UNION:
        for (org.apache.avro.Schema memberSchema : schema.getTypes()) {
          if (!memberSchema.getType().equals(org.apache.avro.Schema.Type.NULL)) {
            return valueSpecToAvro(spec, memberSchema);
          }
        }
      default:
        throw new RuntimeException(
            "This test does not support the data type yet: " + schema.getType().getName());
    }
  }

  @SuppressWarnings("unchecked")
  static Object avroToValueSpec(Object avro, org.apache.avro.Schema schema, boolean toUpper) {
    if (avro == null) {
      return null;
    }
    switch (schema.getType()) {
      case INT:
      case LONG:
      case DOUBLE:
      case FLOAT:
      case BOOLEAN:
        return avro;
      case STRING:
        return avro.toString();
      case ARRAY:
        return ((List)avro).stream()
            .map(o -> avroToValueSpec(o, schema.getElementType(), toUpper))
            .collect(Collectors.toList());
      case MAP:
        return ((Map<Object, Object>)avro).entrySet().stream().collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> avroToValueSpec(e.getValue(), schema.getValueType(), toUpper)
                )
            );
      case RECORD:
        return schema.getFields().stream()
            .collect(
                Collectors.toMap(
                    f -> toUpper ? f.name().toUpperCase() : f.name(),
                    f -> avroToValueSpec(
                        ((GenericData.Record)avro).get(f.name()),
                        f.schema(),
                        toUpper)
                )
            );
      case UNION:
        final int pos = GenericData.get().resolveUnion(schema, avro);
        return avroToValueSpec(avro, schema.getTypes().get(pos), toUpper);
      default:
        throw new RuntimeException("Test cannot handle data of type: " + schema.getType());
    }
  }
}
