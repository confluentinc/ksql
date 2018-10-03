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

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.matchers.JUnitMatchers.isThrowable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.ImmutableMap;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.function.UdfLoaderUtil;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.util.FakeKafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.QueryMetadata;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.apache.kafka.test.TestUtils;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.junit.internal.matchers.ThrowableMessageMatcher;

final class EndToEndEngineTestUtil {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
  private static final String CONFIG_END_MARKER = "CONFIGS_END";

  static {
    // don't use the actual metastore, aim is just to get the functions into the registry.
    // Done once only as it is relatively expensive, i.e., increases the test by 3x if run on each
    // test
    UdfLoaderUtil.load(new MetaStoreImpl(functionRegistry));
  }

  private EndToEndEngineTestUtil(){}

  private static class ValueSpec {
    private final Object spec;

    ValueSpec(final Object spec) {
      this.spec = spec;
    }

    private static void compare(final Object o1, final Object o2, final String path) {
      if (o1 == null && o2 == null) {
        return;
      }
      if (o1 == null || o2 == null) {
        throw new AssertionError("Unexpected null at path " + path);
      }
      if (o1 instanceof Map) {
        assertThat("type mismatch at " + path, o2, instanceOf(Map.class));
        assertThat("keyset mismatch at " + path, ((Map) o1).keySet(), equalTo(((Map)o2).keySet()));
        for (final Object k : ((Map) o1).keySet()) {
          compare(((Map) o1).get(k), ((Map) o2).get(k), path + "." + String.valueOf(k));
        }
      } else if (o1 instanceof List) {
        assertThat("type mismatch at " + path, o2, instanceOf(List.class));
        assertThat("list size mismatch at " + path, ((List) o1).size(), equalTo(((List)o2).size()));
        for (int i = 0; i < ((List) o1).size(); i++) {
          compare(((List) o1).get(i), ((List) o2).get(i), path + "." + String.valueOf(i));
        }
      } else {
        assertThat("type mismatch at " + path, o1.getClass(), equalTo(o2.getClass()));
        assertThat("mismatch at path" + path, o1, equalTo(o2));
      }
    }

    @SuppressWarnings({"EqualsWhichDoesntCheckParameterClass", "Contract"}) // Hack to make work with OutputVerifier.
    @Override
    public boolean equals(final Object o) {
      compare(spec, o, "VALUE-SPEC");
      return Objects.equals(spec, o);
    }
  }

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

    private ValueSpecAvroDeserializer(final SchemaRegistryClient schemaRegistryClient) {
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
      final Object avroObject = avroDeserializer.deserialize(topicName, data);
      final String schemaString;
      try {
        schemaString = schemaRegistryClient.getLatestSchemaMetadata(
            topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX).getSchema();
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
      return new ValueSpec(
          avroToValueSpec(
              avroObject,
              new org.apache.avro.Schema.Parser().parse(schemaString),
              false));
    }
  }

  static class ValueSpecAvroSerializer implements Serializer<Object> {
    private final SchemaRegistryClient schemaRegistryClient;
    private final KafkaAvroSerializer avroSerializer;

    private ValueSpecAvroSerializer(final SchemaRegistryClient schemaRegistryClient) {
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
        schemaString = schemaRegistryClient.getLatestSchemaMetadata(
            topicName + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX).getSchema();
      } catch (final Exception e) {
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
      if (data == null) {
        return null;
      }
      try {
        return new ObjectMapper().readValue(data, Map.class);
      } catch (final Exception e) {
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
      if (spec == null) {
        return null;
      }
      try {
        return new ObjectMapper().writeValueAsBytes(spec);
      } catch (final Exception e) {
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

    SerdeSupplier getSerdeSupplier() {
      return serdeSupplier;
    }

    private Serializer getSerializer(final SchemaRegistryClient schemaRegistryClient) {
      return serdeSupplier.getSerializer(schemaRegistryClient);
    }

    private Deserializer getDeserializer(final SchemaRegistryClient schemaRegistryClient) {
      return serdeSupplier.getDeserializer(schemaRegistryClient);
    }
  }

  static class Window {
    private final long start;
    private final long end;

    Window(final long start, final long end) {
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
    private Deserializer keyDeserializer() {
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

  static class ExpectedException {
    private final List<Matcher<? super Throwable>> matchers = new ArrayList<>();

    public static ExpectedException none() {
      return new ExpectedException();
    }

    public void expect(final Class<? extends Throwable> type) {
      matchers.add(instanceOf(type));
    }

    public void expectMessage(final String substring) {
      expectMessage(containsString(substring));
    }

    public void expectMessage(final Matcher<String> matcher) {
      matchers.add(ThrowableMessageMatcher.hasMessage(matcher));
    }

    private Matcher<Throwable> build() {
      return allOf(matchers);
    }
  }

  static class TestCase {
    private final Path testPath;
    private final JsonNode node;

    TestCase(final Path testPath, final JsonNode node) {
      this.testPath = testPath;
      this.node = node;
    }

    public Path getTestPath() {
      return testPath;
    }

    public JsonNode getNode() {
      return node;
    }
  }

  static class Query {
    private final Path testPath;
    private final String name;
    private final Map<String, Object> properties;
    private final Collection<Topic> topics;
    private final List<Record> inputRecords;
    private final List<Record> outputRecords;
    private final List<String> statements;
    private final ExpectedException expectedException;
    private String generatedTopology;
    private String expectedTopology;
    private Map<String, String> persistedProperties;

    public String getName() {
      return name;
    }

    Query(
        final Path testPath,
        final String name,
        final Map<String, Object> properties,
        final List<Topic> topics,
        final List<Record> inputRecords,
        final List<Record> outputRecords,
        final List<String> statements,
        final ExpectedException expectedException) {
      this.topics = topics;
      this.inputRecords = inputRecords;
      this.outputRecords = outputRecords;
      this.testPath = testPath;
      this.name = name;
      this.properties = ImmutableMap.copyOf(properties);
      this.statements = statements;
      this.expectedException = expectedException;
    }

    void setGeneratedTopology(final String generatedTopology) {
      this.generatedTopology = generatedTopology;
    }

    void setExpectedTopology(final String expectedTopology) {
      this.expectedTopology = expectedTopology;
    }

    void setPersistedProperties(final Map<String, String> persistedProperties) {
       this.persistedProperties = persistedProperties;
    }

    public Optional<Map<String, String>> persistedProperties() {
      return Optional.ofNullable(persistedProperties);
    }

    public Map<String, Object> properties() {
      return properties;
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
      if (isAnyExceptionExpected()) {
        failDueToMissingException();
      }

      int idx = -1;
      try {
        for (idx = 0; idx < outputRecords.size(); idx++) {
          final Record expectedOutput = outputRecords.get(idx);

          final ProducerRecord record = testDriver.readOutput(
              expectedOutput.topic.name,
              expectedOutput.keyDeserializer(),
              expectedOutput.topic.getDeserializer(schemaRegistryClient));

          if (record == null) {
            throw new AssertionError("No record received");
          }

          OutputVerifier.compareKeyValueTimestamp(
              record,
              expectedOutput.key(),
              expectedOutput.value,
              expectedOutput.timestamp);
        }
      } catch (final AssertionError assertionError) {
        final String rowMsg = idx == -1 ? "" : " while processing output row " + idx;
        throw new AssertionError("Query name: "
            + name
            + " in file: " + testPath
            + " failed" + rowMsg + " due to: "
            + assertionError.getMessage());
      }
    }

    void initializeTopics(final KsqlEngine ksqlEngine) {
      for (final Topic topic : topics) {
        ksqlEngine.getTopicClient().createTopic(topic.getName(), 1, (short) 1);
        if (topic.getSchema() != null) {
          try {
            ksqlEngine.getSchemaRegistryClient().register(
                topic.getName() + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX, topic.getSchema());
          } catch (final Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    boolean isAnyExceptionExpected() {
      return !expectedException.matchers.isEmpty();
    }

    private void failDueToMissingException() {
      final String expectation = StringDescription.toString(expectedException.build());
      final String message = "Expected test to throw" + expectation;
      fail(message);
    }

    private void handleException(final RuntimeException e) {
      if (isAnyExceptionExpected()) {
        assertThat(e, isThrowable(expectedException.build()));
      } else {
        throw e;
      }
    }
  }


  static void writeExpectedTopologyFiles(final String topologyDir, List<Query> queryList) {

    final Random randomPort = new Random();
    final ObjectWriter objectWriter = new ObjectMapper().writerWithDefaultPrettyPrinter();
    final MockSchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();
    final Supplier<SchemaRegistryClient> schemaRegistryClientFactory = () -> mockSchemaRegistryClient;
    queryList.forEach(query -> {
      final Map<String, Object> originalConfigs = getConfigs(null);
      final Map<String, Object> updatedConfigs = new HashMap<>(originalConfigs);
      // need to overwrite the bootstrap servers for generating file
      updatedConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + randomPort.nextInt(4000));

      final KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.copyOf(updatedConfigs));
      final KsqlEngine ksqlEngine = getKsqlEngine(schemaRegistryClientFactory, ksqlConfig);
      final Topology topology = getStreamsTopology(query, ksqlEngine, ksqlConfig);
      final Map<String, String> configsToPersist = ksqlConfig.getAllConfigPropsWithSecretsObfuscated();
      writeExpectedTopologyFile(query.name, topology, configsToPersist, objectWriter, topologyDir);
    });
  }



  private static Topology getStreamsTopology(final Query query,
                                             final KsqlEngine ksqlEngine,
                                             final KsqlConfig ksqlConfig) {

      final List<QueryMetadata> queries = new ArrayList<>();
      query.initializeTopics(ksqlEngine);
      query.statements().forEach(
          q -> queries.addAll(
              ksqlEngine.buildMultipleQueries(q, ksqlConfig, query.properties()))
      );

     return queries.get(queries.size() - 1).getTopology();
  }

  private static TopologyTestDriver buildStreamsTopologyTestDriver(final Query query,
                                                                   final KsqlEngine ksqlEngine,
                                                                   final KsqlConfig ksqlConfig,
                                                                   final Properties streamsProperties) {

    final Map<String, String> persistedConfigs = query.persistedProperties().orElse(new HashMap<>());
    final KsqlConfig maybeUpdatedConfigs = persistedConfigs.isEmpty() ? ksqlConfig :
        ksqlConfig.overrideBreakingConfigsWithOriginalValues(persistedConfigs);

    final Topology topology = getStreamsTopology(query, ksqlEngine, maybeUpdatedConfigs);
    if (query.expectedTopology != null) {
      query.setGeneratedTopology(topology.describe().toString());
    }
    return new TopologyTestDriver(topology,
        streamsProperties,
        0);
  }

    private static void writeExpectedTopologyFile(final String queryName,
                                                  final Topology topology,
                                                  final Map<String, String> configs,
                                                  final ObjectWriter objectWriter,
                                                  final String topologyDir) {

        final Path newTopologyDataPath = Paths.get(topologyDir);
        try {
            final String updatedQueryName = formatQueryName(queryName);
            final Path topologyFile = Paths.get(newTopologyDataPath.toString(), updatedQueryName);
            final String configString = objectWriter.writeValueAsString(configs);
            final String topologyString = topology.describe().toString();
            final StringBuilder builder = new StringBuilder();
            builder.append(configString).append("\n").append(CONFIG_END_MARKER).append("\n").append(topologyString);

            final byte[] topologyBytes = builder.toString().getBytes(StandardCharsets.UTF_8);

            Files.write(topologyFile,
                        topologyBytes,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING);


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

  static String formatQueryName(final String originalQueryName) {
    return originalQueryName.replaceAll(" - (AVRO|JSON)$", "").replaceAll("\\s", "_");
  }

  static Map<String, TopologyAndConfigs> loadExpectedTopologies(final String dir) throws IOException {
         final HashMap<String, TopologyAndConfigs> expectedTopologyAndConfigs = new HashMap<>();
         final ObjectReader objectReader = new ObjectMapper().readerFor(Map.class);
         final List<String> topologyFiles = findExpectedTopologyFiles(dir);
         topologyFiles.forEach(fileName -> {
             final TopologyAndConfigs topologyAndConfigs = readTopologyFile(dir + "/" + fileName, objectReader);
             expectedTopologyAndConfigs.put(fileName, topologyAndConfigs);
         });
      return expectedTopologyAndConfigs;
  }

  private static TopologyAndConfigs readTopologyFile(final String file, final ObjectReader objectReader) {
    try (final BufferedReader reader =
        new BufferedReader((
            new InputStreamReader(EndToEndEngineTestUtil.class.getClassLoader().
                getResourceAsStream(file))))) {
      final StringBuilder topologyFileBuilder = new StringBuilder();

      String topologyAndConfigLine;
      Map<String, String> persistedConfigs = null;

      while ((topologyAndConfigLine = reader.readLine()) != null) {
           if (topologyAndConfigLine.contains(CONFIG_END_MARKER)) {
               persistedConfigs = objectReader.readValue(topologyFileBuilder.toString());
               topologyFileBuilder.setLength(0);
           } else {
             topologyFileBuilder.append(topologyAndConfigLine).append("\n");
           }
      }

      return new TopologyAndConfigs(topologyFileBuilder.toString(), persistedConfigs);

    } catch (IOException e) {
      throw new RuntimeException(String.format("Couldn't read topology file %s %s", file, e));
    }
  }

  private static List<String> findExpectedTopologyFiles(final String dir) throws IOException {
       final List<String> topologyFiles = new ArrayList<>();
    try (final BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(EndToEndEngineTestUtil.class.getClassLoader().
                getResourceAsStream(dir)))) {

      String topology;
      while ((topology = reader.readLine()) != null) {
          topologyFiles.add(topology);
      }
    }
    return topologyFiles;
  }

  private static List<Path> findTests(final Path dir) {
    try (final BufferedReader reader = new BufferedReader(
        new InputStreamReader(EndToEndEngineTestUtil.class.getClassLoader().
            getResourceAsStream(dir.toString())))) {

      final List<Path> tests = new ArrayList<>();

      String test;
      while ((test = reader.readLine()) != null) {
        if (test.endsWith(".json")) {
          tests.add(dir.resolve(test));
        }
      }
      return tests;
    } catch (IOException e) {
      throw new AssertionError("Invalid test - failed to read dir: " + dir);
    }
  }

  static Stream<TestCase> findTestCases(final Path dir) {
    final ClassLoader classLoader = EndToEndEngineTestUtil.class.getClassLoader();

    return findTests(dir).stream()
        .flatMap(testPath -> {
          final JsonNode rootNode = loadTest(classLoader, testPath);

          final Spliterator<JsonNode> tests = Spliterators.spliteratorUnknownSize(
              rootNode.findValue("tests").elements(), Spliterator.ORDERED);

          return StreamSupport.stream(tests, false)
              .map(jsonNode -> new TestCase(testPath, jsonNode));
        });
  }

  private static JsonNode loadTest(final ClassLoader classLoader, final Path testPath) {
    try {
      return OBJECT_MAPPER.readTree(classLoader.getResourceAsStream(testPath.toString()));
    } catch (IOException e) {
      throw new RuntimeException("Unable to load test at path " + testPath, e);
    }
  }

  private static KsqlEngine getKsqlEngine(final Supplier<SchemaRegistryClient> clientSupplier,
                                          final KsqlConfig ksqlConfig) {
      final MetaStore metaStore = new MetaStoreImpl(functionRegistry);

     return new KsqlEngine(
          new FakeKafkaTopicClient(),
          clientSupplier,
          metaStore,
          ksqlConfig);
  }

  private static Map<String, Object> getConfigs(final Map<String, Object> additionalConfigs) {

    ImmutableMap.Builder<String, Object> mapBuilder = ImmutableMap.<String, Object>builder()
        .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:0")
        .put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 0)
        .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)
        .put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath())
        .put(StreamsConfig.APPLICATION_ID_CONFIG, "some.ksql.service.id")
        .put(KsqlConfig.KSQL_SERVICE_ID_CONFIG, "some.ksql.service.id")
        .put(StreamsConfig.TOPOLOGY_OPTIMIZATION, "all");

      if(additionalConfigs != null){
          mapBuilder.putAll(additionalConfigs);
      }
      return mapBuilder.build();

  }

  static void shouldBuildAndExecuteQuery(final Query query) {
    final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    final Supplier<SchemaRegistryClient> schemaRegistryClientFactory = () -> schemaRegistryClient;

    final Map<String, Object> config = getConfigs(new HashMap<>());
    final Properties streamsProperties = new Properties();
    streamsProperties.putAll(config);
    final KsqlConfig currentConfigs = new KsqlConfig(config);

    final Map<String, String> persistedConfigs = query.persistedProperties().orElse(new HashMap<>());

    final KsqlConfig ksqlConfig = persistedConfigs.isEmpty() ? currentConfigs :
        currentConfigs.overrideBreakingConfigsWithOriginalValues(persistedConfigs);


    try (final KsqlEngine ksqlEngine = getKsqlEngine(schemaRegistryClientFactory, ksqlConfig)) {
      query.initializeTopics(ksqlEngine);
      final TopologyTestDriver testDriver = buildStreamsTopologyTestDriver(query, ksqlEngine, ksqlConfig, streamsProperties);
      assertEquals(query.expectedTopology, query.generatedTopology);
      query.processInput(testDriver, schemaRegistryClient);
      query.verifyOutput(testDriver, schemaRegistryClient);
      ksqlEngine.close();
    } catch (final RuntimeException e) {
      query.handleException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private static Object valueSpecToAvro(final Object spec, final org.apache.avro.Schema schema) {
    if (spec == null) {
      return null;
    }
    switch (schema.getType()) {
      case INT:
        return Integer.valueOf(spec.toString());
      case LONG:
        return Long.valueOf(spec.toString());
      case STRING:
        return spec.toString();
      case DOUBLE:
        return Double.valueOf(spec.toString());
      case FLOAT:
        return Float.valueOf(spec.toString());
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
        for (final org.apache.avro.Schema.Field field : schema.getFields()) {
          record.put(
              field.name(),
              valueSpecToAvro(((Map<String, ?>)spec).get(field.name()), field.schema())
          );
        }
        return record;
      case UNION:
        for (final org.apache.avro.Schema memberSchema : schema.getTypes()) {
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
  static Object avroToValueSpec(final Object avro,
                                final org.apache.avro.Schema schema,
                                final boolean toUpper) {
    if (avro == null) {
      return null;
    }
    switch (schema.getType()) {
      case INT:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
        return avro;
      case LONG:
        // Ensure that smaller long values match the value spec from the test file.
        // The json deserializer uses Integer for any number less than Integer.MAX_VALUE.
        if (((Long)avro) < Integer.MAX_VALUE && ((Long)avro) > Integer.MIN_VALUE) {
          return ((Long)avro).intValue();
        }
        return avro;
      case ENUM:
      case STRING:
        return avro.toString();
      case ARRAY:
        if (schema.getElementType().getName().equals(AvroData.MAP_ENTRY_TYPE_NAME)) {
          final org.apache.avro.Schema valueSchema
              = schema.getElementType().getField("value").schema();
          return ((List) avro).stream().collect(
              Collectors.toMap(
                  m -> ((GenericData.Record) m).get("key").toString(),
                  m -> (avroToValueSpec(((GenericData.Record) m).get("value"), valueSchema, toUpper))
              )
          );
        }
        return ((List)avro).stream()
            .map(o -> avroToValueSpec(o, schema.getElementType(), toUpper))
            .collect(Collectors.toList());
      case MAP:
        return ((Map<Object, Object>)avro).entrySet().stream().collect(
                Collectors.toMap(
                    e -> e.getKey().toString(),
                    e -> avroToValueSpec(e.getValue(), schema.getValueType(), toUpper)
                )
            );
      case RECORD:
        final Map<String, Object> recordSpec = new HashMap<>();
        schema.getFields().forEach(
            f -> recordSpec.put(
                toUpper ? f.name().toUpperCase() : f.name(),
                avroToValueSpec(
                    ((GenericData.Record)avro).get(f.name()),
                    f.schema(),
                    toUpper)
            )
        );
        return recordSpec;
      case UNION:
        final int pos = GenericData.get().resolveUnion(schema, avro);
        return avroToValueSpec(avro, schema.getTypes().get(pos), toUpper);
      default:
        throw new RuntimeException("Test cannot handle data of type: " + schema.getType());
    }
  }

  static class TopologyAndConfigs {
      public final String topology;
      public final Map<String, String> configs;

    public TopologyAndConfigs(final String topology,
                              final Map<String, String> configs) {
      this.topology = topology;
      this.configs = configs;
    }
  }
}
