/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */
package io.confluent.ksql;

import static java.util.Objects.requireNonNull;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.matchers.JUnitMatchers.isThrowable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.ksql.EndToEndEngineTestUtil.WindowData.Type;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.SessionWindowedDeserializer;
import org.apache.kafka.streams.kstream.SessionWindowedSerializer;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedSerializer;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.apache.kafka.test.TestUtils;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.junit.internal.matchers.ThrowableMessageMatcher;

final class EndToEndEngineTestUtil {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String CONFIG_END_MARKER = "CONFIGS_END";
  private static final String SCHEMAS_END_MARKER = "SCHEMAS_END";

  // Pass a single test or multiple tests separated by commas to the test framework.
  // Example:
  //     mvn test -pl ksql-engine -Dtest=QueryTranslationTest -Dksql.test.files=test1.json
  //     mvn test -pl ksql-engine -Dtest=QueryTranslationTest -Dksql.test.files=test1.json,test2,json
  private static final String KSQL_TEST_FILES = "ksql.test.files";

  private EndToEndEngineTestUtil(){}

  private static class ValueSpec {
    private final Object spec;

    ValueSpec(final Object spec) {
      this.spec = spec;
    }

    @SuppressWarnings("ConstantConditions")
    private static void compare(final Object o1, final Object o2, final String path) {
      if (o1 == null && o2 == null) {
        return;
      }
      if (o1 instanceof Map) {
        assertThat("type mismatch at " + path, o2, instanceOf(Map.class));
        assertThat("keyset mismatch at " + path, ((Map) o1).keySet(), equalTo(((Map)o2).keySet()));
        for (final Object k : ((Map) o1).keySet()) {
          compare(((Map) o1).get(k), ((Map) o2).get(k), path + "." + k);
        }
      } else if (o1 instanceof List) {
        assertThat("type mismatch at " + path, o2, instanceOf(List.class));
        assertThat("list size mismatch at " + path, ((List) o1).size(), equalTo(((List)o2).size()));
        for (int i = 0; i < ((List) o1).size(); i++) {
          compare(((List) o1).get(i), ((List) o2).get(i), path + "." + i);
        }
      } else {
        assertThat("mismatch at path " + path, o1, equalTo(o2));
        assertThat("type mismatch at " + path, o1.getClass(), equalTo(o2.getClass()));
      }
    }

    @SuppressFBWarnings("HE_EQUALS_USE_HASHCODE")
    @SuppressWarnings({"EqualsWhichDoesntCheckParameterClass", "Contract"}) // Hack to make work with OutputVerifier.
    @Override
    public boolean equals(final Object o) {
      compare(spec, o, "VALUE-SPEC");
      return Objects.equals(spec, o);
    }

    @Override
    public int hashCode() {
      return Objects.hash(spec);
    }

    @Override
    public String toString() {
      return Objects.toString(spec);
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
        return OBJECT_MAPPER.readValue(data, Map.class);
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
        return OBJECT_MAPPER.writeValueAsBytes(spec);
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
    private final Optional<org.apache.avro.Schema> schema;
    private final SerdeSupplier serdeSupplier;
    private final int numPartitions;
    private final int replicas;

    Topic(
        final String name,
        final Optional<org.apache.avro.Schema> schema,
        final SerdeSupplier serdeSupplier,
        final int numPartitions,
        final int replicas
    ) {
      this.name = requireNonNull(name, "name");
      this.schema = requireNonNull(schema, "schema");
      this.serdeSupplier = requireNonNull(serdeSupplier, "serdeSupplier");
      this.numPartitions = numPartitions;
      this.replicas = replicas;
    }

    String getName() {
      return name;
    }

    Optional<org.apache.avro.Schema> getSchema() {
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

  static class WindowData {

    enum Type {SESSION, TIME}

    final long start;
    final long end;
    final Type type;

    WindowData(
        @JsonProperty("start") final long start,
        @JsonProperty("end") final long end,
        @JsonProperty("type") final String type
    ) {
      this.start = start;
      this.end = end;
      this.type = Type.valueOf(requireNonNull(type, "type").toUpperCase());
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
    private final WindowData window;

    Record(final Topic topic,
           final String key,
           final Object value,
           final long timestamp,
           final WindowData window) {
      this.topic = topic;
      this.key = key;
      this.value = value;
      this.timestamp = timestamp;
      this.window = window;
    }

    private Serializer<?> keySerializer() {
      final Serializer<String> stringDe = Serdes.String().serializer();
      if (window == null) {
        return stringDe;
      }

      return window.type == Type.SESSION
          ? new SessionWindowedSerializer<>(stringDe)
          : new TimeWindowedSerializer<>(stringDe);
    }

    @SuppressWarnings("unchecked")
    private Deserializer keyDeserializer() {
      if (window == null) {
        return Serdes.String().deserializer();
      }

      final Deserializer<String> inner = Serdes.String().deserializer();
      return window.type == Type.SESSION
          ? new SessionWindowedDeserializer<>(inner)
          : new TimeWindowedDeserializer<>(inner, window.size());
    }

    @SuppressWarnings("unchecked")
    public <W> W key() {
      if (window == null) {
        return (W) key;
      }

      final Window w = window.type == Type.SESSION
          ? new SessionWindow(this.window.start, this.window.end)
          : new TimeWindow(this.window.start, this.window.end);
      return (W) new Windowed<>(key, w);
    }

    public Object value() {
      return value;
    }

    public long timestamp() {
      return timestamp;
    }
  }

  @SuppressFBWarnings("NM_CLASS_NOT_EXCEPTION")
  static class ExpectedException {
    private final List<Matcher<?>> matchers = new ArrayList<>();

    public static ExpectedException none() {
      return new ExpectedException();
    }

    public void expect(final Class<? extends Throwable> type) {
      matchers.add(instanceOf(type));
    }

    public void expect(final Matcher<?> matcher) {
      matchers.add(matcher);
    }

    public void expectMessage(final String substring) {
      expectMessage(containsString(substring));
    }

    public void expectMessage(final Matcher<String> matcher) {
      matchers.add(ThrowableMessageMatcher.hasMessage(matcher));
    }

    @SuppressWarnings("unchecked")
    private Matcher<Throwable> build() {
      return allOf(new ArrayList(matchers));
    }
  }

  static class TestCase implements Test {

    private final Path testPath;
    private final String name;
    private final Map<String, Object> properties;
    private final Collection<Topic> topics;
    private final List<Record> inputRecords;
    private final List<Record> outputRecords;
    private final List<String> statements;
    private final ExpectedException expectedException;
    private String generatedTopology;
    private String generatedSchemas;
    private Optional<TopologyAndConfigs> expectedTopology = Optional.empty();
    private final PostConditions postConditions;

    @Override
    public String getName() {
      return name;
    }

    @Override
    public String getTestFile() {
      return testPath.toString();
    }

    TestCase(
        final Path testPath,
        final String name,
        final Map<String, Object> properties,
        final Collection<Topic> topics,
        final List<Record> inputRecords,
        final List<Record> outputRecords,
        final List<String> statements,
        final ExpectedException expectedException,
        final PostConditions postConditions
    ) {
      this.topics = topics;
      this.inputRecords = inputRecords;
      this.outputRecords = outputRecords;
      this.testPath = testPath;
      this.name = name;
      this.properties = ImmutableMap.copyOf(properties);
      this.statements = statements;
      this.expectedException = expectedException;
      this.postConditions = Objects.requireNonNull(postConditions, "postConditions");
    }

    TestCase copyWithName(String newName) {
      final TestCase copy = new TestCase(
          testPath,
          newName,
          properties,
          topics,
          inputRecords,
          outputRecords,
          statements,
          expectedException,
          postConditions);
      copy.generatedTopology = generatedTopology;
      copy.expectedTopology = expectedTopology;
      copy.generatedSchemas = generatedSchemas;
      return copy;
    }

    void setGeneratedTopology(final String generatedTopology) {
      this.generatedTopology = Objects.requireNonNull(generatedTopology, "generatedTopology");
    }

    void setExpectedTopology(final TopologyAndConfigs expectedTopology) {
      this.expectedTopology = Optional.of(expectedTopology);
    }

    void setGeneratedSchemas(final String generatedSchemas) {
      this.generatedSchemas = Objects.requireNonNull(generatedSchemas, "generatedSchemas");
    }

    Map<String, String> persistedProperties() {
      return expectedTopology
          .flatMap(t -> t.configs)
          .orElseGet(HashMap::new);
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
                  r.keySerializer(),
                  r.topic.getSerializer(schemaRegistryClient)
              ).create(r.topic.name, r.key(), r.value, r.timestamp)
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
        final String topicMsg = idx == -1 ? "" : " topic: " + outputRecords.get(idx).topic.name;
        throw new AssertionError("TestCase name: "
            + name
            + " in file: " + testPath
            + " failed" + rowMsg + topicMsg + " due to: "
            + assertionError.getMessage(), assertionError);
      }
    }

    void initializeTopics(final ServiceContext serviceContext) {
      for (final Topic topic : topics) {
        serviceContext.getTopicClient().createTopic(topic.getName(), topic.numPartitions, (short) topic.replicas);

        topic.getSchema()
            .ifPresent(schema -> {
              try {
                serviceContext.getSchemaRegistryClient()
                    .register(topic.getName() + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX, schema);
              } catch (final Exception e) {
                throw new RuntimeException(e);
              }
            });
      }
    }

    void verifyMetastore(final MetaStore metaStore) {
      postConditions.verify(metaStore);
    }

    void verifyTopology() {
      expectedTopology.ifPresent(expected -> {

        final String expectedTopology = standardizeTopology(expected.topology);
        final String actualTopology = standardizeTopology(generatedTopology);

        assertThat("Generated topology differs from that built by previous versions of KSQL"
                + " - this likely means there is a non-backwards compatible change.\n"
                + "THIS IS BAD!",
            actualTopology, is(expectedTopology));

        expected.schemas.ifPresent(schemas -> {
          assertThat("Schemas used by topology differ from those used by previous versions"
                  + " of KSQL - this likely means there is a non-backwards compatible change.\n"
                  + "THIS IS BAD!",
              generatedSchemas, is(schemas));
        });
      });
    }

    /**
     * Convert a string topology into a standard form.
     *
     * <p>The standardized form takes known compatible changes into account.
     */
    private static String standardizeTopology(final String topology) {
      final String[] lines = topology.split(System.lineSeparator());
      final Pattern aggGroupBy = Pattern.compile("(.*)(--> |<-- |Processor: )Aggregate-groupby(.*)");
      final Pattern linePattern = Pattern.compile("(.*)((?:KSTREAM|KTABLE)-.+-)(\\d+)(.*)");

      final StringBuilder result = new StringBuilder();
      final AtomicInteger nodeCounter = new AtomicInteger();
      final Map<String, String> nodeMappings = new HashMap<>();

      for (String line : lines) {
        final java.util.regex.Matcher aggGroupMatcher = aggGroupBy.matcher(line);
        if (aggGroupMatcher.matches()) {
          line = aggGroupMatcher.group(1)
              + aggGroupMatcher.group(2)
              + "KSTREAM-KEY-SELECT-99999"
              + aggGroupMatcher.group(3);
        }

        final java.util.regex.Matcher mainMatcher = linePattern.matcher(line);
        if (mainMatcher.matches()) {
          final String originalNodeType = mainMatcher.group(2);
          final Integer originalNodeNumber = Integer.valueOf(mainMatcher.group(3));

          final String originalId = originalNodeType + originalNodeNumber;
          final String standardizedId = nodeMappings
              .computeIfAbsent(originalId, key -> originalNodeType + nodeCounter.getAndIncrement());

          line = mainMatcher.group(1) + standardizedId + mainMatcher.group(4);
        }

        result
            .append(line)
            .append(System.lineSeparator());
      }

      return result.toString();
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

  @SuppressWarnings("unchecked")
  static class PostConditions {

    static final PostConditions NONE = new PostConditions(hasItems(anything()));

    final Matcher<Iterable<DataSource<?>>> sourcesMatcher;

    PostConditions(
        final Matcher<Iterable<DataSource<?>>> sourcesMatcher
    ) {
      this.sourcesMatcher = Objects.requireNonNull(sourcesMatcher, "sourcesMatcher");
    }

    public void verify(final MetaStore metaStore) {
      final Collection<DataSource<?>> values = metaStore
          .getAllStructuredDataSources()
          .values();

      final String text = values.stream()
          .map(s -> s.getDataSourceType() + ":" + s.getName()
              + ", key:" + s.getKeyField()
              + ", value:" + s.getSchema())
          .collect(Collectors.joining(System.lineSeparator()));

      assertThat("metastore sources after the statements have run:"
          + System.lineSeparator() + text, values, sourcesMatcher);
    }
  }

  static void writeExpectedTopologyFiles(
      final String topologyDir,
      final List<TestCase> testCases) {

    final ObjectWriter objectWriter = OBJECT_MAPPER.writerWithDefaultPrettyPrinter();

    testCases.forEach(testCase -> {
      final KsqlConfig ksqlConfig = new KsqlConfig(baseConfig())
          .cloneWithPropertyOverwrite(testCase.properties());

      try (final ServiceContext serviceContext = getServiceContext();
          final KsqlEngine ksqlEngine = getKsqlEngine(serviceContext)) {

        final PersistentQueryMetadata queryMetadata =
            buildQuery(testCase, serviceContext, ksqlEngine, ksqlConfig);
        final Map<String, String> configsToPersist
            = new HashMap<>(ksqlConfig.getAllConfigPropsWithSecretsObfuscated());

        // Ignore the KStreams state directory as its different every time:
        configsToPersist.remove("ksql.streams.state.dir");

        writeExpectedTopologyFile(
            testCase.name,
            queryMetadata,
            configsToPersist,
            objectWriter,
            topologyDir);
      }
    });
  }

  private static PersistentQueryMetadata buildQuery(
      final TestCase testCase,
      final ServiceContext serviceContext,
      final KsqlEngine ksqlEngine,
      final KsqlConfig ksqlConfig
  ) {
    testCase.initializeTopics(serviceContext);

    final String sql = testCase.statements().stream()
        .collect(Collectors.joining(System.lineSeparator()));

    final List<QueryMetadata> queries = KsqlEngineTestUtil.execute(
        ksqlEngine,
        sql,
        ksqlConfig,
        Collections.emptyMap(),
        Optional.of(serviceContext.getSchemaRegistryClient())
    );

    assertThat("test did not generate any queries.", queries.isEmpty(), is(false));
    return (PersistentQueryMetadata) queries.get(queries.size() - 1);
  }

  private static KsqlConfig buildConfig(final TestCase testCase) {
    final KsqlConfig baseConfig = new KsqlConfig(baseConfig());

    final Map<String, String> persistedConfigs = testCase.persistedProperties();

    final KsqlConfig compatibleConfig = persistedConfigs.isEmpty() ? baseConfig :
        baseConfig.overrideBreakingConfigsWithOriginalValues(persistedConfigs);

    return compatibleConfig
        .cloneWithPropertyOverwrite(testCase.properties());
  }

  private static TopologyTestDriver buildStreamsTopologyTestDriver(
      final TestCase testCase,
      final ServiceContext serviceContext,
      final KsqlEngine ksqlEngine
  ) {
    final KsqlConfig ksqlConfig = buildConfig(testCase);

    final PersistentQueryMetadata queryMetadata =
        buildQuery(testCase, serviceContext, ksqlEngine, ksqlConfig);

    testCase.setGeneratedTopology(queryMetadata.getTopologyDescription());
    testCase.setGeneratedSchemas(queryMetadata.getSchemasDescription());

    final Properties streamsProperties = new Properties();
    streamsProperties.putAll(queryMetadata.getStreamsProperties());

    return new TopologyTestDriver(
        queryMetadata.getTopology(),
        streamsProperties,
        0);
  }

  private static void writeExpectedTopologyFile(
      final String queryName,
      final PersistentQueryMetadata query,
      final Map<String, String> configs,
      final ObjectWriter objectWriter,
      final String topologyDir
  ) {
      final Path newTopologyDataPath = Paths.get(topologyDir);
      try {
          final String updatedQueryName = formatQueryName(queryName);
          final Path topologyFile = Paths.get(newTopologyDataPath.toString(), updatedQueryName);
          final String configString = objectWriter.writeValueAsString(configs);
        final String topologyString = query.getTopology().describe().toString();
        final String schemasString = query.getSchemasDescription();

        final byte[] topologyBytes =
            (configString + "\n"
                + CONFIG_END_MARKER + "\n"
                + schemasString + "\n"
                + SCHEMAS_END_MARKER + "\n"
                + topologyString
            ).getBytes(StandardCharsets.UTF_8);

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

  static Map<String, TopologyAndConfigs> loadExpectedTopologies(final String dir) {
    final HashMap<String, TopologyAndConfigs> expectedTopologyAndConfigs = new HashMap<>();
    final ObjectReader objectReader = OBJECT_MAPPER.readerFor(Map.class);
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
                getResourceAsStream(file), StandardCharsets.UTF_8)))) {
      final StringBuilder topologyFileBuilder = new StringBuilder();

      String schemas = null;
      String topologyAndConfigLine;
      Optional<Map<String, String>> persistedConfigs = Optional.empty();

      while ((topologyAndConfigLine = reader.readLine()) != null) {
        if (topologyAndConfigLine.contains(CONFIG_END_MARKER)) {
          persistedConfigs = Optional
              .of(objectReader.readValue(topologyFileBuilder.toString()));
          topologyFileBuilder.setLength(0);
        } else if (topologyAndConfigLine.contains(SCHEMAS_END_MARKER)) {
          schemas = StringUtils.stripEnd(topologyFileBuilder.toString(), "\n");
          topologyFileBuilder.setLength(0);
        } else {
          topologyFileBuilder.append(topologyAndConfigLine).append("\n");
        }
      }

      return new TopologyAndConfigs(
          topologyFileBuilder.toString(),
          Optional.ofNullable(schemas),
          persistedConfigs
      );

    } catch (IOException e) {
      throw new RuntimeException(String.format("Couldn't read topology file %s %s", file, e));
    }
  }

  static List<String> findExpectedTopologyDirectories(final String dir) {
    try {
      return findContentsOfDirectory(dir);
    } catch (final IOException e) {
      throw new RuntimeException("Could not find expected topology directories.", e);
    }
  }

  private static List<String> findExpectedTopologyFiles(final String dir) {
    try {
      return findContentsOfDirectory(dir);
    } catch (final IOException e) {
      throw new RuntimeException("Could not find expected topology files. dir: " + dir, e);
    }
  }

  private static List<String> findContentsOfDirectory(final String dir) throws IOException {
    final List<String> contents = new ArrayList<>();
    try (final BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(EndToEndEngineTestUtil.class.getClassLoader().
                getResourceAsStream(dir), StandardCharsets.UTF_8))) {

      String file;
      while ((file = reader.readLine()) != null) {
        contents.add(file);
      }
    }
    return contents;
  }

  private static List<Path> findTests(final Path dir) {
    try (final BufferedReader reader = new BufferedReader(
        new InputStreamReader(EndToEndEngineTestUtil.class.getClassLoader().
            getResourceAsStream(dir.toString()), StandardCharsets.UTF_8))) {

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

  private static List<Path> getTests(final Path dir, final List<String> files) {
    return files.stream().map(name -> dir.resolve(name.trim())).collect(Collectors.toList());
  }

  /**
   * Returns a list of files specified in the system property 'ksql.test.file'.
   * The list may be specified as a comma-separated string. If 'ksql.test.file' is not found,
   * then an empty list is returned.
   */
  static List<String> getTestFilesParam() {
    final String ksqlTestFiles = System.getProperty(KSQL_TEST_FILES, "").trim();
    if (ksqlTestFiles.isEmpty()) {
      return Collections.emptyList();
    }

    return Arrays.asList(ksqlTestFiles.split(","));
  }

  interface Test {

    String getName();

    String getTestFile();
  }

  interface TestFile<TestType extends Test> {

    Stream<TestType> buildTests(final Path testPath);
  }

  static <TF extends TestFile<T>, T extends Test> Stream<T> findTestCases(
      final Path dir,
      final List<String> files,
      final Class<TF> testFileType
  ) {
    final List<T> testCases = getTestPaths(dir, files).stream()
        .flatMap(testPath -> buildTests(testPath, testFileType))
        .collect(Collectors.toList());

    throwOnDuplicateNames(testCases);

    return testCases.stream();
  }

  private static void throwOnDuplicateNames(final List<? extends Test> testCases) {
    final String duplicates = testCases.stream()
        .collect(Collectors.groupingBy(Test::getName))
        .entrySet()
        .stream()
        .filter(e -> e.getValue().size() > 1)
        .map(e -> "test name: '" + e.getKey()
            + "' found in files: " + e.getValue().stream().map(Test::getTestFile)
            .collect(Collectors.joining(",")))
        .collect(Collectors.joining(System.lineSeparator()));

    if (!duplicates.isEmpty()) {
      throw new IllegalStateException("There are tests with duplicate names: "
          + System.lineSeparator() + duplicates);
    }
  }

  /**
   * Return a list of test paths found on the given directory. If the files parameter is not empty,
   * then returns only the paths of the given list.
   */
  private static List<Path> getTestPaths(final Path dir, final List<String> files) {
    if (files != null && !files.isEmpty()) {
      return getTests(dir, files);
    } else {
      return findTests(dir);
    }
  }

  private static <TF extends TestFile<T>, T extends Test> Stream<T> buildTests(
      final Path testPath,
      final Class<TF> testFileType
  ) {
    try (InputStream stream = EndToEndEngineTestUtil.class
        .getClassLoader()
        .getResourceAsStream(testPath.toString())
    ) {
      final TF testFile = OBJECT_MAPPER.readValue(stream, testFileType);
      return testFile.buildTests(testPath);
    } catch (Exception e) {
      throw new RuntimeException("Unable to load test at path " + testPath, e);
    }
  }

  private static ServiceContext getServiceContext() {
    final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    return TestServiceContext.create(() -> schemaRegistryClient);
  }

  private static KsqlEngine getKsqlEngine(final ServiceContext serviceContext) {
    final MutableMetaStore metaStore = new MetaStoreImpl(TestFunctionRegistry.INSTANCE.get());
    return KsqlEngineTestUtil.createKsqlEngine(serviceContext, metaStore);
  }

  private static Map<String, Object> baseConfig() {
    return ImmutableMap.<String, Object>builder()
        .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:0")
        .put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 0)
        .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)
        .put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath())
        .put(StreamsConfig.APPLICATION_ID_CONFIG, "some.ksql.service.id")
        .put(KsqlConfig.KSQL_SERVICE_ID_CONFIG, "some.ksql.service.id")
        .put(KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS,
            KsqlConfig.KSQL_USE_NAMED_INTERNAL_TOPICS_ON)
        .put(StreamsConfig.TOPOLOGY_OPTIMIZATION, "all")
        .build();
  }

  static void shouldBuildAndExecuteQuery(final TestCase testCase) {

    try (final ServiceContext serviceContext = getServiceContext();
        final KsqlEngine ksqlEngine = getKsqlEngine(serviceContext)
    ) {
      testCase.initializeTopics(serviceContext);

      final TopologyTestDriver testDriver = buildStreamsTopologyTestDriver(
          testCase,
          serviceContext,
          ksqlEngine);

      testCase.verifyTopology();
      testCase.processInput(testDriver, serviceContext.getSchemaRegistryClient());
      testCase.verifyOutput(testDriver, serviceContext.getSchemaRegistryClient());
      testCase.verifyMetastore(ksqlEngine.getMetaStore());
    } catch (final RuntimeException e) {
      testCase.handleException(e);
    } catch (final AssertionError e) {
      throw new AssertionError("test: " + testCase.getName() + System.lineSeparator()
          + e.getMessage(), e);
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
        throw new RuntimeException("Union must have non-null type: " + schema.getType().getName());

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
        if (schema.getElementType().getName().equals(AvroData.MAP_ENTRY_TYPE_NAME) ||
            Objects.equals(
                schema.getElementType().getProp(AvroData.CONNECT_INTERNAL_TYPE_NAME),
                AvroData.MAP_ENTRY_TYPE_NAME)
            ) {
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
        final boolean hasNull = schema.getTypes().stream()
            .anyMatch(s -> s.getType().equals(org.apache.avro.Schema.Type.NULL));
        final Object resolved = avroToValueSpec(avro, schema.getTypes().get(pos), toUpper);
        if (schema.getTypes().get(pos).getType().equals(org.apache.avro.Schema.Type.NULL)
          || schema.getTypes().size() == 2 && hasNull) {
          return resolved;
        }
        final Map<String, Object> ret = Maps.newHashMap();
        schema.getTypes()
            .forEach(
              s -> ret.put(s.getName().toUpperCase(), null));
        ret.put(schema.getTypes().get(pos).getName().toUpperCase(), resolved);
        return ret;
      default:
        throw new RuntimeException("Test cannot handle data of type: " + schema.getType());
    }
  }

  static class TopologyAndConfigs {

    final String topology;
    final Optional<String> schemas;
    final Optional<Map<String, String>> configs;

    TopologyAndConfigs(
        final String topology,
        final Optional<String> schemas,
        final Optional<Map<String, String>> configs
    ) {
      this.topology = Objects.requireNonNull(topology, "topology");
      this.schemas = Objects.requireNonNull(schemas, "schemas");
      this.configs = Objects.requireNonNull(configs, "configs");
    }
  }

  @SuppressWarnings("UnstableApiUsage")
  static String buildTestName(
      final Path testPath,
      final String testName,
      final String postfix
  ) {
    final String fileName = com.google.common.io.Files.getNameWithoutExtension(testPath.toString());
    final String pf = postfix.isEmpty() ? "" : " - " + postfix;
    return fileName + " - " + testName + pf;
  }

  static Optional<org.apache.avro.Schema> buildAvroSchema(final JsonNode schema) {
    if (schema instanceof NullNode) {
      return Optional.empty();
    }

    try {
      final String schemaString = OBJECT_MAPPER.writeValueAsString(schema);
      final org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
      return Optional.of(parser.parse(schemaString));
    } catch (final Exception e) {
      throw new InvalidFieldException("schema", "failed to parse", e);
    }
  }

  static final class MissingFieldException extends RuntimeException {

    MissingFieldException(final String fieldName) {
      super("test must define '" + fieldName + "' field");
    }
  }

  static final class InvalidFieldException extends RuntimeException {

    InvalidFieldException(
        final String fieldName,
        final String reason
    ) {
      super(fieldName + ": " + reason);
    }

    InvalidFieldException(
        final String fieldName,
        final String reason,
        final Throwable cause
    ) {
      super(fieldName + ": " + reason, cause);
    }
  }
}
