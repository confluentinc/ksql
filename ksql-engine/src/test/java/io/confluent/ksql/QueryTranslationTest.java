package io.confluent.ksql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.confluent.ksql.EndToEndEngineTestUtil.ExpectedException;
import io.confluent.ksql.serde.DataSource;

import static io.confluent.ksql.EndToEndEngineTestUtil.Query;
import static io.confluent.ksql.EndToEndEngineTestUtil.Record;
import static io.confluent.ksql.EndToEndEngineTestUtil.SerdeSupplier;
import static io.confluent.ksql.EndToEndEngineTestUtil.StringSerdeSupplier;
import static io.confluent.ksql.EndToEndEngineTestUtil.Topic;
import static io.confluent.ksql.EndToEndEngineTestUtil.ValueSpecAvroSerdeSupplier;
import static io.confluent.ksql.EndToEndEngineTestUtil.ValueSpecJsonSerdeSupplier;
import static io.confluent.ksql.EndToEndEngineTestUtil.Window;
import static io.confluent.ksql.EndToEndEngineTestUtil.findTests;

@RunWith(Parameterized.class)
public class QueryTranslationTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final String QUERY_VALIDATION_TEST_DIR = "query-validation-tests";

  private final Query query;

  /**
   * @param name  - unused. Is just so the tests get named.
   * @param query - query to run.
   */
  @SuppressWarnings("unused")
  public QueryTranslationTest(final String name, final Query query) {
    this.query = query;
  }

  @Test
  public void shouldBuildAndExecuteQueries() {
    EndToEndEngineTestUtil.shouldBuildAndExecuteQuery(this.query);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() throws IOException {
    final List<String> testFiles = findTests(QUERY_VALIDATION_TEST_DIR);
    return testFiles.stream().flatMap(test -> {
      final String testPath = QUERY_VALIDATION_TEST_DIR + "/" + test;
      final JsonNode tests;
      try {
        tests = objectMapper.readTree(
            EndToEndEngineTestUtil.class.getClassLoader().
                getResourceAsStream(testPath));
      } catch (IOException e) {
        throw new RuntimeException("Unable to load test at path " + testPath);
      }
      final List<Query> queries = new ArrayList<>();
      tests.findValue("tests").elements().forEachRemaining(query -> {
        try {
          final String name = getRequiredQueryField("Unknown", query,"name").asText();
          final Map<String, Object> properties = new HashMap<>();
          final List<String> statements = new ArrayList<>();
          final List<Record> inputs = new ArrayList<>();
          final List<Record> outputs = new ArrayList<>();
          final List<Topic> topics = new LinkedList<>();
          final ExpectedException expectedException = ExpectedException.none();

          final JsonNode propertiesNode = query.findValue("properties");
          if (propertiesNode != null) {
            propertiesNode.fields()
                .forEachRemaining(property -> properties.put(property.getKey(), property.getValue().asText()));
          }
          topics.addAll(
              Arrays.asList(
                  new Topic("test_topic", null, new StringSerdeSupplier()),
                  new Topic("test_table", null, new StringSerdeSupplier()),
                  new Topic("left_topic", null, new StringSerdeSupplier()),
                  new Topic("right_topic", null, new StringSerdeSupplier())
              )
          );

          getRequiredQueryField(name, query, "statements").elements()
              .forEachRemaining(statement -> statements.add(statement.asText()));

          if (query.has("topics")) {
            query.findValue("topics").forEach(
                topic -> topics.add(createTopicFromNode(topic))
            );
          }

          if (query.has("expectedException")) {
            final JsonNode node = query.findValue("expectedException");
            if (node.hasNonNull("type")) {
              expectedException.expect(parseThrowable(name, node.get("type").asText()));
            }
            if (node.hasNonNull("message")) {
              expectedException.expectMessage(node.get("message").asText());
            }
          }

          getRequiredQueryField(name, query,"inputs").elements()
              .forEachRemaining(input -> inputs.add(createRecordFromNode(topics, input)));

          getRequiredQueryField(name, query,"outputs").elements()
              .forEachRemaining(output -> outputs.add(createRecordFromNode(topics, output)));

          queries.add(
              new Query(testPath, name, properties, topics, inputs, outputs, statements, expectedException));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
      return queries.stream()
          .map(query -> new Object[]{query.getName(), query});
    }).collect(Collectors.toCollection(ArrayList::new));
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

  private static SerdeSupplier getSerdeSupplier(final String format) {
    switch(format) {
      case DataSource.AVRO_SERDE_NAME:
        return new ValueSpecAvroSerdeSupplier();
      case DataSource.JSON_SERDE_NAME:
        return new ValueSpecJsonSerdeSupplier();
      case DataSource.DELIMITED_SERDE_NAME:
      default:
        return new StringSerdeSupplier();
    }
  }

  private static Topic createTopicFromNode(final JsonNode node) {
    final org.apache.avro.Schema schema;
    if (node.has("schema")) {
      try {
        final String schemaString = objectMapper.writeValueAsString(node.get("schema"));
        final org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        schema = parser.parse(schemaString);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    } else {
      schema = null;
    }

    final SerdeSupplier serdeSupplier = getSerdeSupplier(node.get("format").asText());

    return new Topic(node.get("name").asText(), schema, serdeSupplier);
  }

  private static Record createRecordFromNode(final List<Topic> topics, final JsonNode node) {
    final String topicName = node.findValue("topic").asText();
    final Topic topic = topics.stream()
        .filter(t -> t.getName().equals(topicName))
        .findFirst()
        .orElse(new Topic(topicName, null, new StringSerdeSupplier()));

    final Object topicValue;
    if (node.findValue("value").asText().equals("null")) {
      topicValue = null;
    } else if (topic.getSerdeSupplier() instanceof StringSerdeSupplier) {
      topicValue = node.findValue("value").asText();
    } else {
      try {
        topicValue = objectMapper.readValue(
            objectMapper.writeValueAsString(node.findValue("value")), Object.class);
      } catch (IOException e) {
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
}
