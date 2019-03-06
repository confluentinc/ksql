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
import static io.confluent.ksql.EndToEndEngineTestUtil.findExpectedTopologyDirectories;
import static io.confluent.ksql.EndToEndEngineTestUtil.formatQueryName;
import static io.confluent.ksql.EndToEndEngineTestUtil.loadExpectedTopologies;
import static io.confluent.ksql.util.KsqlExceptionMatcher.statementText;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import io.confluent.connect.avro.AvroData;
import io.confluent.ksql.EndToEndEngineTestUtil.JsonTestCase;
import io.confluent.ksql.EndToEndEngineTestUtil.TestCase;
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
import io.confluent.ksql.schema.ksql.LogicalSchemas;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.StringUtil;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
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
    final List<TopologiesAndVersion> expectedTopologies = loadTopologiesAndVersions();
    return buildTestCases()
          .flatMap(q -> buildVersionedTestCases(q, expectedTopologies))
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

    return EndToEndEngineTestUtil.findTestCases(QUERY_VALIDATION_TEST_DIR, testFiles)
        .flatMap(test -> {
          final JsonNode formatsNode = test.getNode().get("format");
          if (formatsNode == null) {
            return Stream.of(createTest(test, ""));
          }

          final Spliterator<JsonNode> formats = Spliterators.spliteratorUnknownSize(
              formatsNode.iterator(), Spliterator.ORDERED);

          return StreamSupport.stream(formats, false)
              .map(format -> createTest(test, format.asText()));
        });
  }

  private static TestCase createTest(final JsonTestCase test, final String format) {
    final String testName = buildTestName(test, format);

    try {
      final JsonNode testCase = test.getNode();

      final Map<String, Object> properties = getTestCaseProperties(testCase);

      final List<String> statements = getTestCaseStatements(testCase, format);

      final Optional<ExpectedException> expectedException =
          getTestCaseExpectedException(testCase, Iterables.getLast(statements));

      final Map<String, Topic> topics =
          getTestCaseTopics(testCase, statements, expectedException.isPresent());

      final List<Record> inputs = getTestCaseTopicMessages(testCase, topics, "inputs");

      final List<Record> outputs = getTestCaseTopicMessages(testCase, topics, "outputs");

      if (inputs.isEmpty() && !expectedException.isPresent()) {
        throw new InvalidFieldException("inputs", "is empty and 'expectedException' is not defind");
      }

      return new TestCase(
          test.getTestPath(),
          testName,
          properties,
          topics.values(),
          inputs,
          outputs,
          statements,
          expectedException.orElseGet(ExpectedException::none)
      );
    } catch (final Exception e) {
      throw new AssertionError(testName + ": Invalid test. " + e.getMessage(), e);
    }
  }

  private static List<Record> getTestCaseTopicMessages(
      final JsonNode testCase,
      final Map<String, Topic> topics,
      final String fieldName
  ) {
    final List<Record> messages = new ArrayList<>();
    getOptionalJsonField(testCase, fieldName)
        .ifPresent(node -> node.elements().forEachRemaining(
            message -> messages.add(
                createRecordFromNode(topics, message, fieldName + "[]"))));

    return messages;
  }

  private static Map<String, Topic> getTestCaseTopics(
      final JsonNode testCase,
      final List<String> statements,
      final boolean expectsException
  ) {
    final Map<String, Topic> topicsMap = new HashMap<>();

    // Add all topics from topic nodes to the map
    getOptionalJsonField(testCase, "topics")
        .ifPresent(topicsNode -> topicsNode.forEach(
            topicNode -> {
              final Topic topic = createTopicFromNode(topicNode);
              topicsMap.put(topic.getName(), createTopicFromNode(topicNode));
            }
        ));

    // Infer topics if not added already
    statements.stream()
        .map(QueryTranslationTest::createTopicFromStatement)
        .filter(Objects::nonNull)
        .forEach(
            topic -> topicsMap.putIfAbsent(topic.getName(), topic)
        );

    if (topicsMap.isEmpty()) {
      if (expectsException) {
        return topicsMap;
      }
      throw new InvalidFieldException("statements/topics", "The test does not define any topics");
    }

    final SerdeSupplier defaultSerdeSupplier =
        topicsMap.values().iterator().next().getSerdeSupplier();

    // Get topics from inputs field:
    final Set<String> msgTopics = getTestCaseMessageTopicNames(testCase, "inputs");
    msgTopics.addAll(getTestCaseMessageTopicNames(testCase, "outputs"));

    msgTopics.stream()
        .filter(topicName -> !topicsMap.containsKey(topicName))
        .forEach(topicName -> topicsMap
            .put(topicName, (new Topic(topicName, Optional.empty(), defaultSerdeSupplier, 4))));

    return topicsMap;
  }

  private static Set<String> getTestCaseMessageTopicNames(
      final JsonNode testCase,
      final String fieldName
  ) {
    final Set<String> allTopics = new HashSet<>();

    getOptionalJsonField(testCase, fieldName)
        .ifPresent(messages -> messages.elements().forEachRemaining(
            message -> allTopics
                .add(getRequiredJsonField(message, "topic", fieldName + "[]").asText())));

    return allTopics;
  }

  private static List<String> getTestCaseStatements(final JsonNode testCase, final String format) {
    final List<String> statements = new ArrayList<>();
    getRequiredJsonField(testCase, "statements").elements()
        .forEachRemaining(
            statement -> statements.add(statement.asText().replace("{FORMAT}", format)));
    return statements;
  }

  private static Map<String, Object> getTestCaseProperties(final JsonNode testCase) {
    final Map<String, Object> properties = new HashMap<>();
    getOptionalJsonField(testCase, "properties")
        .ifPresent(propNode -> propNode.fields().forEachRemaining(
            property -> properties.put(property.getKey(), property.getValue().asText())));

    return properties;
  }

  private static Optional<ExpectedException> getTestCaseExpectedException(
      final JsonNode testCase,
      final String lastStatement
  ) {
    return getOptionalJsonField(testCase, "expectedException")
        .map(eeNode -> {
          final ExpectedException expectedException = ExpectedException.none();

          getOptionalJsonField(eeNode, "type")
              .map(JsonNode::asText)
              .map(QueryTranslationTest::parseThrowable)
              .ifPresent(type -> {
                expectedException.expect(type);

                if (type.equals(KsqlStatementException.class)) {
                  // Ensure exception contains last statement, otherwise the test case is invalid:
                  expectedException.expect(statementText(is(lastStatement)));
                }
              });

          getOptionalJsonField(eeNode, "message")
              .ifPresent(msg -> expectedException.expectMessage(msg.asText()));

          return expectedException;
        });
  }

  private static String buildTestName(final JsonTestCase testCase, final String format) {
    try {
      final String fileName = Files.getNameWithoutExtension(testCase.getTestPath().toString());
      final String testName = getRequiredJsonField(testCase.getNode(), "name").asText();
      final String formatPostFix = format.isEmpty() ? "" : " - " + format;

      return fileName + " - " + testName + formatPostFix;
    } catch (final MissingFieldException e) {
      throw new AssertionError(
          "Test file contains an invalid test case: " + testCase.getTestPath(), e);
    }
  }

  private static SerdeSupplier getSerdeSupplier(final String format) {
    switch(format.toUpperCase()) {
      case DataSource.AVRO_SERDE_NAME:
        return new ValueSpecAvroSerdeSupplier();
      case DataSource.JSON_SERDE_NAME:
        return new ValueSpecJsonSerdeSupplier();
      case DataSource.DELIMITED_SERDE_NAME:
      default:
        return new StringSerdeSupplier();
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
        statement.getElements().forEach(e -> schemaBuilder.field(
            e.getName(),
            LogicalSchemas.fromSqlTypeConverter().fromSqlType(e.getType()))
        );
        avroSchema = Optional.of(new AvroData(1)
            .fromConnectSchema(addNames(schemaBuilder.build())));
      } else {
        avroSchema = Optional.empty();
      }
      return new Topic(topicName, avroSchema, getSerdeSupplier(format), KsqlConstants.legacyDefaultSinkPartitionCount);
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

  private static Topic createTopicFromNode(final JsonNode node) {
    final Optional<org.apache.avro.Schema> schema;
    if (node.has("schema")) {
      try {
        final String schemaString = objectMapper.writeValueAsString(node.get("schema"));
        final org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        schema = Optional.of(parser.parse(schemaString));
      } catch (final JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    } else {
      schema = Optional.empty();
    }

    final SerdeSupplier serdeSupplier = getSerdeSupplier(node.get("format").asText());

    final int numPartitions = node.has("partitions")
        ? node.get("partitions").intValue()
        : 1;

    return new Topic(node.get("name").asText(), schema, serdeSupplier, numPartitions);
  }

  private static Record createRecordFromNode(
      final Map<String, Topic> topics,
      final JsonNode node,
      final String scope
  ) {
    final String topicName = getRequiredJsonField(node, "topic", scope).asText();

    final String key = getOptionalJsonField(node, "key")
        .map(JsonNode::asText)
        .orElse("");

    final Object topicValue;
    if (node.findValue("value").asText().equals("null")) {
      topicValue = null;
    } else if (topics.get(topicName).getSerdeSupplier() instanceof StringSerdeSupplier) {
      topicValue = node.findValue("value").asText();
    } else {
      try {
        topicValue = objectMapper.readValue(
            objectMapper.writeValueAsString(node.findValue("value")), Object.class);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }

    final long timestamp = getOptionalJsonField(node, "timestamp")
        .map(JsonNode::asLong)
        .orElse(0L);

    final WindowData window = createWindowIfExists(node);


    return new Record(
        topics.get(topicName),
        key,
        topicValue,
        timestamp,
        window
    );
  }

  private static WindowData createWindowIfExists(final JsonNode node) {
    final JsonNode windowNode = node.findValue("window");
    if (windowNode == null) {
      return null;
    }

    return new WindowData(
        windowNode.findValue("start").asLong(),
        windowNode.findValue("end").asLong(),
        windowNode.findValue("type").asText());
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends Throwable> parseThrowable(final String className) {
    try {
      final Class<?> theClass = Class.forName(className);
      if (!Throwable.class.isAssignableFrom(theClass)) {
        throw new InvalidFieldException("expectedException.type", "Type was not a Throwable");
        }
        return (Class<? extends Throwable>) theClass;
    } catch (final ClassNotFoundException e) {
      throw new InvalidFieldException("expectedException.type", "Type was not found", e);
    }
  }

  private static JsonNode getRequiredJsonField(
      final JsonNode query,
      final String fieldName
  ) {
    return getRequiredJsonField(query, fieldName, "");
  }

  private static JsonNode getRequiredJsonField(
      final JsonNode query,
      final String fieldName,
      final String scope
  ) {
    if (!query.has(fieldName)) {
      throw new MissingFieldException(scope + "." + fieldName);
    }
    return query.findValue(fieldName);
  }

  private static Optional<JsonNode> getOptionalJsonField(
      final JsonNode node,
      final String fieldName
  ) {
    if (node.hasNonNull(fieldName)) {
      return Optional.of(node.findValue(fieldName));
    }
    return Optional.empty();
  }

  private static final class MissingFieldException extends RuntimeException {

    private MissingFieldException(final String fieldName) {
      super("test it must define '" + fieldName + "' field");
    }
  }

  private static final class InvalidFieldException extends RuntimeException {

    private InvalidFieldException(
        final String fieldName,
        final String reason
    ) {
      super("'" + fieldName + "': " + reason);
    }

    private InvalidFieldException(
        final String fieldName,
        final String reason,
        final Throwable cause
    ) {
      super(fieldName + ": " + reason, cause);
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
}
