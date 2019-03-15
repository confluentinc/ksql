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

import static io.confluent.ksql.EndToEndEngineTestUtil.ExpectedException;
import static io.confluent.ksql.EndToEndEngineTestUtil.Record;
import static io.confluent.ksql.EndToEndEngineTestUtil.SerdeSupplier;
import static io.confluent.ksql.EndToEndEngineTestUtil.StringSerdeSupplier;
import static io.confluent.ksql.EndToEndEngineTestUtil.Topic;
import static io.confluent.ksql.EndToEndEngineTestUtil.ValueSpecAvroSerdeSupplier;
import static io.confluent.ksql.EndToEndEngineTestUtil.ValueSpecJsonSerdeSupplier;
import static io.confluent.ksql.EndToEndEngineTestUtil.buildTestName;
import static io.confluent.ksql.EndToEndEngineTestUtil.findExpectedTopologyDirectories;
import static io.confluent.ksql.EndToEndEngineTestUtil.formatQueryName;
import static io.confluent.ksql.EndToEndEngineTestUtil.loadExpectedTopologies;
import static io.confluent.ksql.util.KsqlExceptionMatcher.statementText;
import static java.util.Objects.requireNonNull;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import io.confluent.connect.avro.AvroData;
import io.confluent.ksql.EndToEndEngineTestUtil.InvalidFieldException;
import io.confluent.ksql.EndToEndEngineTestUtil.MissingFieldException;
import io.confluent.ksql.EndToEndEngineTestUtil.TestCase;
import io.confluent.ksql.EndToEndEngineTestUtil.TestFile;
import io.confluent.ksql.EndToEndEngineTestUtil.TopologyAndConfigs;
import io.confluent.ksql.EndToEndEngineTestUtil.WindowData;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.StringUtil;
import io.confluent.ksql.util.TypeUtil;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *  Runs the json functional tests defined under
 *  `ksql-engine/src/test/resources/query-validation-tests`.
 *
 *  See `ksql-engine/src/test/resources/query-validation-tests/README.md` for more info.
 */
@RunWith(Parameterized.class)
public class QueryTranslationTest {

  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final Path QUERY_VALIDATION_TEST_DIR = Paths.get("query-validation-tests");
  private static final String TOPOLOGY_CHECKS_DIR = "expected_topology/";
  private static final String TOPOLOGY_VERSIONS_DELIMITER = ",";
  private static final String TOPOLOGY_VERSIONS_PROP = "topology.versions";

  private final TestCase testCase;

  /**
   * @param name  - unused. Is just so the tests get named.
   * @param testCase - testCase to run.
   */
  @SuppressWarnings("unused")
  public QueryTranslationTest(final String name, final TestCase testCase) {
    this.testCase = Objects.requireNonNull(testCase, "testCase");
  }

  @Test
  public void shouldBuildAndExecuteQueries() {
    EndToEndEngineTestUtil.shouldBuildAndExecuteQuery(testCase);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return buildTestCases()
        .map(testCase -> new Object[]{testCase.getName(), testCase})
        .collect(Collectors.toCollection(ArrayList::new));
  }

  private static List<TopologiesAndVersion> loadTopologiesAndVersions() {
    return Stream.of(getTopologyVersions())
        .map(version ->
            new TopologiesAndVersion(version, loadExpectedTopologies(TOPOLOGY_CHECKS_DIR + version)))
        .collect(Collectors.toList());
  }

  private static String[] getTopologyVersions() {
    String[] topologyVersions;
    final String topologyVersionsProp = System.getProperty(TOPOLOGY_VERSIONS_PROP);
    if (topologyVersionsProp != null) {
      topologyVersions = topologyVersionsProp.split(TOPOLOGY_VERSIONS_DELIMITER);
    } else {
      final List<String> topologyVersionsList = findExpectedTopologyDirectories(TOPOLOGY_CHECKS_DIR);
      topologyVersions = new String[topologyVersionsList.size()];
      topologyVersions = topologyVersionsList.toArray(topologyVersions);
    }
    return topologyVersions;
  }

  private static Stream<TestCase> buildVersionedTestCases(
      final TestCase testCase, final List<TopologiesAndVersion> expectedTopologies) {
    Stream.Builder<TestCase> builder = Stream.builder();
    builder = builder.add(testCase);

    for (final TopologiesAndVersion topologies : expectedTopologies) {
      final TopologyAndConfigs topologyAndConfigs =
          topologies.getTopology(formatQueryName(testCase.getName()));
      // could be null if the testCase has expected errors, no topology or configs saved
      if (topologyAndConfigs != null) {
        final TestCase versionedTestCase = testCase.copyWithName(
            testCase.getName() + "-" + topologies.getVersion());
        versionedTestCase.setExpectedTopology(topologyAndConfigs.topology);
        versionedTestCase.setPersistedProperties(topologyAndConfigs.configs);
        builder = builder.add(versionedTestCase);
      }
    }
    return builder.build();
  }

  static Stream<TestCase> buildTestCases() {
    final List<String> testFiles = EndToEndEngineTestUtil.getTestFilesParam();
    final List<TopologiesAndVersion> expectedTopologies = loadTopologiesAndVersions();

    return EndToEndEngineTestUtil.findTestCases(
        QUERY_VALIDATION_TEST_DIR,
        testFiles,
        QttTestFile.class)
        .flatMap(q -> buildVersionedTestCases(q, expectedTopologies));
  }

  private static SerdeSupplier getSerdeSupplier(final String format) {
    switch(format.toUpperCase()) {
      case DataSource.AVRO_SERDE_NAME:
        return new ValueSpecAvroSerdeSupplier();
      case DataSource.JSON_SERDE_NAME:
        return new ValueSpecJsonSerdeSupplier();
      case DataSource.DELIMITED_SERDE_NAME:
        return new StringSerdeSupplier();
      default:
        throw new InvalidFieldException("format", format.isEmpty()
            ? "missing or empty"
            : "unknown value: " + format);
    }
  }

  private static Schema addNames(final Schema schema) {
    final SchemaBuilder builder;
    switch(schema.type()) {
      case ARRAY:
        builder = SchemaBuilder.array(addNames(schema.valueSchema()));
        break;
      case MAP:
        builder = SchemaBuilder.map(
            Schema.STRING_SCHEMA,
            addNames(schema.valueSchema())
        );
        break;
      case STRUCT:
        builder = SchemaBuilder.struct();
        builder.name("TestSchema" + UUID.randomUUID().toString().replace("-", ""));
        for (final Field field : schema.fields()) {
          builder.field(field.name(), addNames(field.schema()));
        }
        break;
      default:
        builder = SchemaBuilder.type(schema.type());
    }
    if (schema.isOptional()) {
      builder.optional();
    }
    return builder.build();
  }

  private static Topic createTopicFromStatement(final String sql) {
    final KsqlParser parser = new DefaultKsqlParser();
    final MetaStoreImpl metaStore = new MetaStoreImpl(new InternalFunctionRegistry());

    final Predicate<ParsedStatement> onlyCSandCT = stmt ->
        stmt.getStatement().statement() instanceof SqlBaseParser.CreateStreamContext
            || stmt.getStatement().statement() instanceof SqlBaseParser.CreateTableContext;

    final Function<PreparedStatement<?>, Topic> extractTopic = stmt -> {
      final AbstractStreamCreateStatement statement = (AbstractStreamCreateStatement) stmt
          .getStatement();

      final Map<String, Expression> properties = statement.getProperties();
      final String topicName
          = StringUtil.cleanQuotes(properties.get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY).toString());
      final String format
          = StringUtil.cleanQuotes(properties.get(DdlConfig.VALUE_FORMAT_PROPERTY).toString());

      final Optional<org.apache.avro.Schema> avroSchema;
      if (format.equals(DataSource.AVRO_SERDE_NAME)) {
        // add avro schema
        final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        statement.getElements().forEach(
            e -> schemaBuilder.field(e.getName(), TypeUtil.getTypeSchema(e.getType()))
        );
        avroSchema = Optional.of(new AvroData(1)
            .fromConnectSchema(addNames(schemaBuilder.build())));
      } else {
        avroSchema = Optional.empty();
      }
      return new Topic(topicName, avroSchema, getSerdeSupplier(format), 1, 1);
    };

    try {
      final List<ParsedStatement> parsed = parser.parse(sql);
      if (parsed.size() > 1) {
        throw new IllegalArgumentException("SQL contains more than one statement: " + sql);
      }

      final List<Topic> topics = parsed.stream()
          .filter(onlyCSandCT)
          .map(stmt -> parser.prepare(stmt, metaStore))
          .map(extractTopic)
          .collect(Collectors.toList());

      return topics.isEmpty() ? null : topics.get(0);
    } catch (final ParseFailedException e) {
      // Statement won't parse:
      return null;
    }
  }

  private static class TopologiesAndVersion {

    private final String version;
    private final Map<String, TopologyAndConfigs> topologies;

    TopologiesAndVersion(final String version, final Map<String, TopologyAndConfigs> topologies) {
      this.version = version;
      this.topologies = topologies;
    }

    String getVersion() {
      return version;
    }

    TopologyAndConfigs getTopology(final String name) {
      return topologies.get(name);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static class QttTestFile implements TestFile<TestCase> {

    private final List<TestCaseNode> tests;

    QttTestFile(@JsonProperty("tests") final List<TestCaseNode> tests) {
      this.tests = ImmutableList.copyOf(requireNonNull(tests, "tests collection missing"));
    }

    @Override
    public Stream<TestCase> buildTests(final Path testPath) {
      if (tests.isEmpty()) {
        throw new IllegalArgumentException(testPath + ": test file did not contain any tests");
      }

      try {
        return tests.stream().flatMap(node -> node.buildTests(testPath));
      } catch (final Exception e) {
        throw new IllegalArgumentException(testPath + ": " + e.getMessage(), e);
      }
    }
  }

  static final class ExpectedExceptionNode {

    private final Optional<String> type;
    private final Optional<String> message;

    ExpectedExceptionNode(
        @JsonProperty("type") final String type,
        @JsonProperty("message") final String message
    ) {
      this.type = Optional.ofNullable(type);
      this.message = Optional.ofNullable(message);
    }

    ExpectedException build(final String lastStatement) {
      if (!type.isPresent() && !message.isPresent()) {
        throw new MissingFieldException("expectedException.type or expectedException.message");
      }

      final ExpectedException expectedException = ExpectedException.none();

      type
          .map(t -> EndToEndEngineTestUtil.parseThrowable(t, "expectedException.type"))
          .ifPresent(type -> {
            expectedException.expect(type);

            if (KsqlStatementException.class.isAssignableFrom(type)) {
              // Ensure exception contains last statement, otherwise the test case is invalid:
              expectedException.expect(statementText(is(lastStatement)));
            }
          });

      message.ifPresent(expectedException::expectMessage);
      return expectedException;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static class TestCaseNode {

    private final String name;
    private final List<String> formats;
    private final List<RecordNode> inputs;
    private final List<RecordNode> outputs;
    private final List<TopicNode> topics;
    private final List<String> statements;
    private final Map<String, Object> properties;
    private final Optional<ExpectedExceptionNode> expectedException;

    TestCaseNode(
        @JsonProperty("name") final String name,
        @JsonProperty("format") final List<String> formats,
        @JsonProperty("inputs") final List<RecordNode> inputs,
        @JsonProperty("outputs") final List<RecordNode> outputs,
        @JsonProperty("topics") final List<TopicNode> topics,
        @JsonProperty("statements") final List<String> statements,
        @JsonProperty("properties") final Map<String, Object> properties,
        @JsonProperty("expectedException") ExpectedExceptionNode expectedException
    ) {
      this.name = name == null ? "" : name;
      this.formats = formats == null ? ImmutableList.of() : ImmutableList.copyOf(formats);
      this.statements = statements == null ? ImmutableList.of() : ImmutableList.copyOf(statements);
      this.inputs = inputs == null ? ImmutableList.of() : ImmutableList.copyOf(inputs);
      this.outputs = outputs == null ? ImmutableList.of() : ImmutableList.copyOf(outputs);
      this.topics = topics == null ? ImmutableList.of() : ImmutableList.copyOf(topics);
      this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
      this.expectedException = Optional.ofNullable(expectedException);
    }

    Stream<TestCase> buildTests(final Path testPath) {
      if (name.isEmpty()) {
        throw new MissingFieldException("name");
      }

      try {
        if (statements.isEmpty()) {
          throw new InvalidFieldException("statements", "was empty");
        }

        if (inputs.isEmpty() != outputs.isEmpty()) {
          throw new InvalidFieldException("inputs and outputs",
              "either both, or neither, field should be set");
        }

        if (!inputs.isEmpty() && expectedException.isPresent()) {
          throw new InvalidFieldException("inputs and expectedException",
              "can not both be set");
        }

        return formats.isEmpty()
            ? Stream.of(createTest("", testPath))
            : formats.stream().map(format -> createTest(format, testPath));
      } catch (final Exception e) {
        throw new IllegalArgumentException("test '" + name + "': " + e.getMessage(), e);
      }
    }

    private TestCase createTest(final String format, final Path testPath) {
      final String testName = buildTestName(testPath, name, format);

      try {
        final List<String> statements = buildStatements(format);

        final Optional<ExpectedException> ee = buildExpectedException(statements);

        final Map<String, Topic> topics = getTestCaseTopics(statements, ee.isPresent());

        final List<Record> inputRecords = inputs.stream()
            .map(node -> node.build(topics))
            .collect(Collectors.toList());

        final List<Record> outputRecords = outputs.stream()
            .map(node -> node.build(topics))
            .collect(Collectors.toList());

        return new TestCase(
            testPath,
            testName,
            properties,
            topics.values(),
            inputRecords,
            outputRecords,
            statements,
            ee.orElseGet(ExpectedException::none)
        );
      } catch (final Exception e) {
        throw new AssertionError(testName + ": Invalid test. " + e.getMessage(), e);
      }
    }

    private Optional<ExpectedException> buildExpectedException(final List<String> statements) {
      return this.expectedException
          .map(ee -> ee.build(Iterables.getLast(statements)));
    }

    private List<String> buildStatements(final String format) {
      return statements.stream()
          .map(stmt -> stmt.replace("{FORMAT}", format))
          .collect(Collectors.toList());
    }

    private Map<String, Topic> getTestCaseTopics(
        final List<String> statements,
        final boolean expectsException
    ) {
      final Map<String, Topic> allTopics = new HashMap<>();

      // Add all topics from topic nodes to the map:
      topics.stream()
          .map(TopicNode::build)
          .forEach(topic -> allTopics.put(topic.getName(), topic));

      // Infer topics if not added already:
      statements.stream()
          .map(QueryTranslationTest::createTopicFromStatement)
          .filter(Objects::nonNull)
          .forEach(
              topic -> allTopics.putIfAbsent(topic.getName(), topic)
          );

      if (allTopics.isEmpty()) {
        if (expectsException) {
          return ImmutableMap.of();
        }
        throw new InvalidFieldException("statements/topics", "The test does not define any topics");
      }

      final SerdeSupplier defaultSerdeSupplier =
          allTopics.values().iterator().next().getSerdeSupplier();

      // Get topics from inputs and outputs fields:
      Streams.concat(inputs.stream(), outputs.stream())
          .map(RecordNode::topicName)
          .map(topicName -> new Topic(topicName, Optional.empty(), defaultSerdeSupplier, 4, 1))
          .forEach(topic -> allTopics.putIfAbsent(topic.getName(), topic));

      return allTopics;
    }
  }

  static class TopicNode {

    private final String name;
    private final String format;
    private final int numPartitions;
    private final JsonNode schema;
    private final int replicas;

    TopicNode(
        @JsonProperty("name") final String name,
        @JsonProperty("schema") final JsonNode schema,
        @JsonProperty("format") final String format,
        @JsonProperty("partitions") final Integer numPartitions,
        @JsonProperty("replicas") final Integer replicas
    ) {
      this.name = name == null ? "" : name;
      this.schema = requireNonNull(schema, "schema");
      this.format = format == null ? "" : format;
      this.numPartitions = numPartitions == null ? 1 : numPartitions;
      this.replicas = replicas == null ? 1 : replicas;
    }

    Topic build() {
      if (name.isEmpty()) {
        throw new InvalidFieldException("name", "empty or missing");
      }

      return new Topic(
          name,
          buildAvroSchema(),
          getSerdeSupplier(format),
          numPartitions,
          replicas
      );
    }

    private Optional<org.apache.avro.Schema> buildAvroSchema() {
      if (schema instanceof NullNode) {
        return Optional.empty();
      }

      try {
        final String schemaString = objectMapper.writeValueAsString(schema);
        final org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        return Optional.of(parser.parse(schemaString));
      } catch (final Exception e) {
        throw new InvalidFieldException("schema", "failed to parse", e);
      }
    }
  }

  static class RecordNode {

    private final String topicName;
    private final String key;
    private final JsonNode value;
    private final long timestamp;
    private final Optional<WindowData> window;

    RecordNode(
        @JsonProperty("topic") final String topicName,
        @JsonProperty("key") final String key,
        @JsonProperty("value") final JsonNode value,
        @JsonProperty("timestamp") final Long timestamp,
        @JsonProperty("window") final WindowData window
    ) {
      this.topicName = topicName == null ? "" : topicName;
      this.key = key == null ? "" : key;
      this.value = requireNonNull(value, "value");
      this.timestamp = timestamp == null ? 0L : timestamp;
      this.window = Optional.ofNullable(window);
    }

    public String topicName() {
      if (topicName.isEmpty()) {
        throw new MissingFieldException("topic");
      }

      return topicName;
    }

    private Record build(final Map<String, Topic> topics) {
      final Topic topic = topics.get(topicName());

      final Object topicValue = buildValue(topic);

      return new Record(
          topic,
          key,
          topicValue,
          timestamp,
          window.orElse(null)
      );
    }

    private Object buildValue(final Topic topic) {
      if (value.asText().equals("null")) {
        return null;
      }

      if (topic.getSerdeSupplier() instanceof StringSerdeSupplier) {
        return value.asText();
      }

      try {
        return objectMapper.readValue(objectMapper.writeValueAsString(value), Object.class);
      } catch (final IOException e) {
        throw new InvalidFieldException("value", "failed to parse", e);
      }
    }
  }
}

