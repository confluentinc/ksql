package io.confluent.ksql;

import static io.confluent.ksql.EndToEndEngineTestUtil.ExpectedException;
import static io.confluent.ksql.EndToEndEngineTestUtil.Query;
import static io.confluent.ksql.EndToEndEngineTestUtil.Record;
import static io.confluent.ksql.EndToEndEngineTestUtil.SerdeSupplier;
import static io.confluent.ksql.EndToEndEngineTestUtil.StringSerdeSupplier;
import static io.confluent.ksql.EndToEndEngineTestUtil.Topic;
import static io.confluent.ksql.EndToEndEngineTestUtil.ValueSpecAvroSerdeSupplier;
import static io.confluent.ksql.EndToEndEngineTestUtil.ValueSpecJsonSerdeSupplier;
import static io.confluent.ksql.EndToEndEngineTestUtil.Window;
import static io.confluent.ksql.EndToEndEngineTestUtil.findTests;
import static io.confluent.ksql.EndToEndEngineTestUtil.formatQueryName;
import static io.confluent.ksql.EndToEndEngineTestUtil.loadExpectedTopologies;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.avro.AvroData;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class QueryTranslationTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final String QUERY_VALIDATION_TEST_DIR = "query-validation-tests";
  private static final String CURRENT_TOPOLOGY_CHECKS_DIR = "5_0_expected_topology";
  private static final String PREVIOUS_TOPOLOGY_CHECKS_DIR_PROP = "topology.dir";

  private final Query query;

  /**
   * @param name  - unused. Is just so the tests get named.
   * @param query - query to run.
   */
  @SuppressWarnings("unused")
  public QueryTranslationTest(final String name, final Query query) {
    this.query = Objects.requireNonNull(query, "query");
  }

  @Test
  public void shouldBuildAndExecuteQueries() {
    EndToEndEngineTestUtil.shouldBuildAndExecuteQuery(this.query);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() throws IOException {
    final String topologyDirectory = System.getProperty(PREVIOUS_TOPOLOGY_CHECKS_DIR_PROP, CURRENT_TOPOLOGY_CHECKS_DIR);
    final Map<String, String> expectedTopologies = loadExpectedTopologies(topologyDirectory);
    return buildQueryList().stream()
          .peek(q -> q.setExpectedTopology(expectedTopologies.get(formatQueryName(q.getName()))))
          .map(query -> new Object[]{query.getName(), query})
          .collect(Collectors.toCollection(ArrayList::new));
  }

  static List<Query> buildQueryList()  throws IOException {
    final List<String> testFiles = findTests(QUERY_VALIDATION_TEST_DIR);

    return testFiles.stream().flatMap(test -> {
      final String testPath = QUERY_VALIDATION_TEST_DIR + "/" + test;
      final JsonNode tests;
      try {
        tests = objectMapper.readTree(
            EndToEndEngineTestUtil.class.getClassLoader().
                getResourceAsStream(testPath));
      } catch (IOException e) {
        throw new RuntimeException("Unable to load test at path " + testPath, e);
      }
      final List<Query> queries = new ArrayList<>();
      tests.findValue("tests").elements().forEachRemaining(query -> {
        final JsonNode formats = query.get("format");
        if (formats == null) {
          queries.add(createTest(testPath, query, ""));
        } else {
          formats.iterator().forEachRemaining(
              format -> queries.add(createTest(testPath, query, format.asText())));
        }
      });
      return queries.stream();
    }).collect(Collectors.toList());
  }

  private static Query createTest(final String testPath, final JsonNode query, final String format) {
    try {
      final StringBuilder nameBuilder = new StringBuilder();
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

      return new Query(testPath, name, properties, topics, inputs, outputs, statements,
          expectedException);
    } catch (final Exception e) {
      throw new RuntimeException("Failed to build a query in " + testPath, e);
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

    final Function<PreparedStatement, Topic> mapper = stmt -> {
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

    return new Record(
        topic,
        node.findValue("key").asText(),
        topicValue,
        node.findValue("timestamp").asLong(),
        createWindowIfExists(node)
    );
  }

  private static Window createWindowIfExists(final JsonNode node) {
    final JsonNode windowNode = node.findValue("window");
    if (windowNode == null) {
      return null;
    }

    return new Window(
        windowNode.findValue("start").asLong(),
        windowNode.findValue("end").asLong());
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
}
