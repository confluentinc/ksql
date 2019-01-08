/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import static io.confluent.ksql.EndToEndEngineTestUtil.formatQueryName;
import static io.confluent.ksql.EndToEndEngineTestUtil.loadExpectedTopologies;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import io.confluent.connect.avro.AvroData;
import io.confluent.ksql.EndToEndEngineTestUtil.JsonTestCase;
import io.confluent.ksql.EndToEndEngineTestUtil.TestCase;
import io.confluent.ksql.EndToEndEngineTestUtil.TopologyAndConfigs;
import io.confluent.ksql.EndToEndEngineTestUtil.WindowData;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.StringUtil;
import io.confluent.ksql.util.TypeUtil;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
 *  This test also validates the generated topology matches the
 *  expected topology. The expected topology files, and the configuration
 *  used to generated them are found in src/test/resources/expected_topology/&lt;Version Number&gt;
 *
 *  By default this test will compare the
 *  current generated topology against the previous released version
 *  identified by the CURRENT_TOPOLOGY_VERSION variable.
 *
 *  To run this test against previously released versions there are three options
 *
 *  1. Just manually change CURRENT_TOPOLOGY_VERSION to a valid version number found under
 *  the src/test/resources/expected_topology directory.
 *
 *  2. This test checks for a system property "topology.version" on test startup. If that
 *  property is set, that is the version used for the test.
 *
 *  3. There are two options for setting the system property.
 *     a. Within Intellij
 *        i. Click Run/Edit configurations
 *        ii. Select the QueryTranslationTest
 *        iii. Enter -Dtopology.version=X  in the "VM options:" form entry
 *             where X is the desired previously released version number.
 *
 *     b. From the command line
 *        i. run mvn clean package -DskipTests=true from the base of the KSQL project
 *        ii. Then run "mvn test -Dtopology.version=X -Dtest=QueryTranslationTest -pl ksql-engine"
 *            (without the quotes).  Again X is the version you want to run the tests against.
 *
 *   Note that for both options above the version must exist
 *   under the src/test/resources/expected_topology directory.
 *
 *  For instructions on how to generate new topologies, see TopologyFileGenerator.java.
 */

@RunWith(Parameterized.class)
public class QueryTranslationTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final Path QUERY_VALIDATION_TEST_DIR = Paths.get("query-validation-tests");
  private static final String TOPOLOGY_CHECKS_DIR = "expected_topology";
  private static final String TOPOLOGY_VERSION_DELIMITER = ",";
  private static final String CURRENT_TOPOLOGY_VERSIONS = "5_0,5_1";
  private static final String TOPOLOGY_VERSION_PROP = "topology.version";

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
    final String[] topologyVersions =
        System.getProperty(TOPOLOGY_VERSION_PROP, CURRENT_TOPOLOGY_VERSIONS)
            .split(TOPOLOGY_VERSION_DELIMITER);
    final List<TopologiesAndVersion> expectedTopologies = loadTopologiesAndVersions(topologyVersions);
    return buildTestCases()
          .flatMap(q -> buildVersionedTestCases(q, expectedTopologies))
          .map(testCase -> new Object[]{testCase.getName(), testCase})
          .collect(Collectors.toCollection(ArrayList::new));
  }

  private static List<TopologiesAndVersion> loadTopologiesAndVersions(final String[] topologyVersions) {
    return Stream.of(topologyVersions)
        .map(topologyVersion -> {
          final String topologyDirectory = TOPOLOGY_CHECKS_DIR + "/" + topologyVersion;
          try {
            return new TopologiesAndVersion(topologyVersion, loadExpectedTopologies(topologyDirectory));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        })
        .collect(Collectors.toList());
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
        final TestCase versionedTestCase = TestCase.copyWithName(
            testCase, testCase.getName() + "-" + topologies.getVersion());
        versionedTestCase.setExpectedTopology(topologyAndConfigs.topology);
        versionedTestCase.setPersistedProperties(topologyAndConfigs.configs);
        builder = builder.add(versionedTestCase);
      }
    }
    return builder.build();
  }

  static Stream<TestCase> buildTestCases() {
    return EndToEndEngineTestUtil.findTestCases(QUERY_VALIDATION_TEST_DIR)
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
    try {
      final JsonNode query = test.getNode();
      final StringBuilder nameBuilder = new StringBuilder();
      nameBuilder.append(Files.getNameWithoutExtension(test.getTestPath().toString()));
      nameBuilder.append(" - ");
      nameBuilder.append(getRequiredQueryField("Unknown", query, "name").asText());
      if (!format.equals("")) {
        nameBuilder.append(" - ").append(format);
      }
      final String name = nameBuilder.toString();
      final Map<String, Object> properties = new HashMap<>();
      final List<String> statements = new ArrayList<>();
      final List<Record> inputs = new ArrayList<>();
      final List<Record> outputs = new ArrayList<>();
      final JsonNode propertiesNode = query.findValue("properties");
      final ExpectedException expectedException = ExpectedException.none();

      if (propertiesNode != null) {
        propertiesNode.fields()
            .forEachRemaining(
                property -> properties.put(property.getKey(), property.getValue().asText()));
      }
      getRequiredQueryField(name, query, "statements").elements()
          .forEachRemaining(
              statement -> statements.add(statement.asText().replace("{FORMAT}", format)));

      final Map<String, Topic> topicsMap = new HashMap<>();
      // add all topics from topic nodes to the map
      if (query.has("topics")) {
        query.findValue("topics").forEach(
            topicNode -> {
              final Topic topic = createTopicFromNode(topicNode);
              topicsMap.put(topic.getName(), createTopicFromNode(topicNode));
            }
        );
      }
      // infer topics if not added already
      statements.stream()
          .map(QueryTranslationTest::createTopicFromStatement)
          .filter(Objects::nonNull)
          .forEach(
              topic -> topicsMap.putIfAbsent(topic.getName(), topic)
          );
      final List<Topic> topics = new LinkedList<>(topicsMap.values());

      final SerdeSupplier defaultSerdeSupplier = topics.get(0).getSerdeSupplier();

      getRequiredQueryField(name, query, "inputs").elements()
          .forEachRemaining(
              input -> inputs.add(createRecordFromNode(topics, input, defaultSerdeSupplier)));

      getRequiredQueryField(name, query, "outputs").elements()
          .forEachRemaining(
              output -> outputs.add(createRecordFromNode(topics, output, defaultSerdeSupplier)));

      if (query.has("expectedException")) {
        final JsonNode node = query.findValue("expectedException");
        if (node.hasNonNull("type")) {
          expectedException.expect(parseThrowable(name, node.get("type").asText()));
        }
        if (node.hasNonNull("message")) {
          expectedException.expectMessage(node.get("message").asText());
        }
      }

      return new TestCase(test.getTestPath(), name, properties, topics, inputs, outputs, statements,
          expectedException);
    } catch (final Exception e) {
      throw new RuntimeException("Failed to build a testCase in " + test.getTestPath(), e);
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
    final KsqlParser parser = new KsqlParser();
    final MetaStoreImpl metaStore = new MetaStoreImpl(new InternalFunctionRegistry());

    final Predicate<ParsedStatement> filter = stmt ->
        stmt.getStatement().statement() instanceof SqlBaseParser.CreateStreamContext
            || stmt.getStatement().statement() instanceof SqlBaseParser.CreateTableContext;

    final Function<PreparedStatement<?>, Topic> mapper = stmt -> {
      final AbstractStreamCreateStatement statement = (AbstractStreamCreateStatement) stmt
          .getStatement();

      final Map<String, Expression> properties = statement.getProperties();
      final String topicName
          = StringUtil.cleanQuotes(properties.get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY).toString());
      final String format
          = StringUtil.cleanQuotes(properties.get(DdlConfig.VALUE_FORMAT_PROPERTY).toString());

      final org.apache.avro.Schema avroSchema;
      if (format.equals(DataSource.AVRO_SERDE_NAME)) {
        // add avro schema
        final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        statement.getElements().forEach(
            e -> schemaBuilder.field(e.getName(), TypeUtil.getTypeSchema(e.getType()))
        );
        avroSchema = new AvroData(1).fromConnectSchema(addNames(schemaBuilder.build()));
      } else {
        avroSchema = null;
      }
      return new Topic(topicName, avroSchema, getSerdeSupplier(format));
    };

    final List<Topic> topics = parser.buildAst(sql, metaStore, filter, mapper);
    return topics.isEmpty() ? null :topics.get(0);
  }

  private static Topic createTopicFromNode(final JsonNode node) {
    final org.apache.avro.Schema schema;
    if (node.has("schema")) {
      try {
        final String schemaString = objectMapper.writeValueAsString(node.get("schema"));
        final org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        schema = parser.parse(schemaString);
      } catch (final JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    } else {
      schema = null;
    }

    final SerdeSupplier serdeSupplier = getSerdeSupplier(node.get("format").asText());

    return new Topic(node.get("name").asText(), schema, serdeSupplier);
  }

  private static Record createRecordFromNode(final List<Topic> topics,
                                             final JsonNode node,
                                             final SerdeSupplier defaultSerdeSupplier) {
    final String topicName = node.findValue("topic").asText();
    final Topic topic = topics.stream()
        .filter(t -> t.getName().equals(topicName))
        .findFirst()
        .orElse(new Topic(topicName, null, defaultSerdeSupplier));

    final Object topicValue;
    if (node.findValue("value").asText().equals("null")) {
      topicValue = null;
    } else if (topic.getSerdeSupplier() instanceof StringSerdeSupplier) {
      topicValue = node.findValue("value").asText();
    } else {
      try {
        topicValue = objectMapper.readValue(
            objectMapper.writeValueAsString(node.findValue("value")), Object.class);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }

    final WindowData window = createWindowIfExists(node);

    return new Record(
        topic,
        node.findValue("key").asText(),
        topicValue,
        node.findValue("timestamp").asLong(),
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
  private static Class<? extends Throwable> parseThrowable(final String testName,
                                                           final String className) {
    try {
      final Class<?> theClass = Class.forName(className);
      if (!Throwable.class.isAssignableFrom(theClass)) {
        throw new AssertionError(testName + ": Invalid test - 'expectedException.type' not Throwable");
        }
        return (Class<? extends Throwable>) theClass;
    } catch (final ClassNotFoundException e) {
      throw new AssertionError(testName + ": Invalid test - 'expectedException.type' not found", e);
    }
  }

  private static JsonNode getRequiredQueryField(final String testName,
                                                final JsonNode query,
                                                final String fieldName) {
    if (!query.has(fieldName)) {
      throw new AssertionError(
          testName + ": Invalid test - it must define '" + fieldName + "' field");
    }
    return query.findValue(fieldName);
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
