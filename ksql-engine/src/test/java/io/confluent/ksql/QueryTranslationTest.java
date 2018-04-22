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

import com.google.common.base.Joiner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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

@RunWith(Parameterized.class)
public class QueryTranslationTest {

  private static final String QUERY_VALIDATION_TEST_DIR = "query-validation-tests";
  private final MetaStore metaStore = new MetaStoreImpl();
  private final Map<String, Object> config = new HashMap<String, Object>() {{
    put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    put("application.id", "KSQL-TEST");
    put("commit.interval.ms", 0);
    put("cache.max.bytes.buffering", 0);
    put("auto.offset.reset", "earliest");
    put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
  }};
  private final Properties streamsProperties = new Properties();
  private final ConsumerRecordFactory<String, String> recordFactory =
      new ConsumerRecordFactory<>(
          Serdes.String().serializer(),
          Serdes.String().serializer());

  private Query query;

  private KsqlEngine ksqlEngine;

  @Before
  public void before() {
    streamsProperties.putAll(config);
    ksqlEngine = new KsqlEngine(new KsqlConfig(config),
        new FakeKafkaTopicClient(),
        new MockSchemaRegistryClient(),
        metaStore);
    ksqlEngine.getTopicClient().createTopic("test_topic", 1, (short) 1);
    ksqlEngine.getTopicClient().createTopic("test_table", 1, (short) 1);
  }

  @After
  public void cleanUp() {
    ksqlEngine.close();
  }

  static class Window {
    private final long start;
    private final long end;

    Window(long start, long end) {
      this.start = start;
      this.end = end;
    }

    public long size() {
      return end - start;
    }
  }

  static class Record {
    private final String topic;
    private final String key;
    private final String value;
    private final long timestamp;
    private final Window window;

    Record(final String topic,
           final String key,
           final String value,
           final long timestamp,
           final Window window) {
      this.topic = topic;
      this.key = key;
      this.value = value.equals("null") ? null : value;
      this.timestamp = timestamp;
      this.window = window;
    }

    public Deserializer keyDeserializer() {
      if (window == null) {
        return Serdes.String().deserializer();
      }
      return new TimeWindowedDeserializer(Serdes.String().deserializer(), window.size());
    }

    @SuppressWarnings("unchecked")
    public <T> T key() {
      if (window == null) {
        return (T) key;
      }
      return (T) new Windowed<>(key, new TimeWindow(window.start, window.end));
    }
  }


  static class Query {
    private final String testPath;
    private final String name;
    private final List<String> statements;
    private final List<Record> inputs;
    private final List<Record> expectedOutputs;

    Query(final String testPath,
          final String name,
          final List<String> statements,
          final List<Record> inputs,
          final List<Record> expectedOutputs) {
      this.testPath = testPath;
      this.name = name;
      this.statements = statements;
      this.inputs = inputs;
      this.expectedOutputs = expectedOutputs;
    }

    public String statements() {
      return Joiner.on("\n").join(statements);
    }

    void processInput(final TopologyTestDriver testDriver,
                      final ConsumerRecordFactory<String, String> recordFactory) {
      inputs.forEach(record -> testDriver.pipeInput(
          recordFactory.create(record.topic,
              record.key,
              record.value,
              record.timestamp))
      );
    }

    @SuppressWarnings("unchecked")
    void verifyOutput(final TopologyTestDriver testDriver) {
      for (final Record expectedOutput : expectedOutputs) {
        try {
          OutputVerifier.compareKeyValueTimestamp(testDriver.readOutput(expectedOutput.topic,
              expectedOutput.keyDeserializer(),
              Serdes.String().deserializer()),
              expectedOutput.key(),
              expectedOutput.value,
              expectedOutput.timestamp);
        } catch (AssertionError assertionError) {
          throw new AssertionError("Query name: "
              + name
              + " in file: " + testPath
              + " failed due to: "
              + assertionError.getMessage());
        }
      }
      // check for no more records
    }
  }

  private TopologyTestDriver buildStreamsTopology(final Query query) throws Exception {
    final List<QueryMetadata> queries = ksqlEngine.buildMultipleQueries(query.statements(),
        Collections.emptyMap());
    return new TopologyTestDriver(queries.get(queries.size() - 1).getTopology(),
        streamsProperties,
        0);
  }


  /**
   * @param name  - unused. Is just so the tests get named.
   * @param query - query to run.
   */
  public QueryTranslationTest(final String name, final Query query) {
    this.query = query;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() throws IOException {
    final List<String> testFiles = findTests();
    return testFiles.stream().flatMap(test -> {
      final ObjectMapper objectMapper = new ObjectMapper();
      final String testPath = QUERY_VALIDATION_TEST_DIR + "/" + test;
      final JsonNode tests;
      try {
        tests = objectMapper.readTree(
            QueryTranslationTest.class.getClassLoader().
                getResourceAsStream(testPath));
      } catch (IOException e) {
        throw new RuntimeException("Unable to load test at path " + testPath);
      }
      final List<Query> queries = new ArrayList<>();
      tests.findValue("tests").elements().forEachRemaining(query -> {
        try {
          final String name = query.findValue("name").asText();
          final List<String> statements = new ArrayList<>();
          final List<Record> inputs = new ArrayList<>();
          final List<Record> outputs = new ArrayList<>();
          query.findValue("statements").elements()
              .forEachRemaining(statement -> statements.add(statement.asText()));
          query.findValue("inputs").elements()
              .forEachRemaining(input -> inputs.add(createRecordFromNode(input)));
          query.findValue("outputs").elements()
              .forEachRemaining(output -> outputs.add(createRecordFromNode(output)));
          queries.add(new Query(testPath, name, statements, inputs, outputs));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
      return queries.stream()
          .map(query -> new Object[]{query.name, query});
    }).collect(Collectors.toCollection(ArrayList::new));
  }

  private static List<String> findTests() throws IOException {
    final List<String> tests = new ArrayList<>();
    try (final BufferedReader reader =
             new BufferedReader(
                 new InputStreamReader(QueryTranslationTest.class.getClassLoader().
                     getResourceAsStream(QUERY_VALIDATION_TEST_DIR)))) {

      String test;
      while ((test = reader.readLine()) != null) {
        if (test.endsWith(".json")) {
          tests.add(test);
        }
      }
    }
    return tests;
  }

  @Test
  public void shouldBuildAndExecuteQuery() throws Exception {
    final TopologyTestDriver testDriver = buildStreamsTopology(query);
    query.processInput(testDriver, recordFactory);
    query.verifyOutput(testDriver);
  }

  private static Record createRecordFromNode(final JsonNode node) {
    return new Record(
        node.findValue("topic").asText(),
        node.findValue("key").asText(),
        node.findValue("value").asText(),
        node.findValue("timestamp").asLong(),
        createWindowIfExists(node));
  }

  private static Window createWindowIfExists(JsonNode node) {
    final JsonNode windowNode = node.findValue("window");
    if (windowNode == null) {
      return null;
    }

    return new Window(
        windowNode.findValue("start").asLong(),
        windowNode.findValue("end").asLong());
  }

}
