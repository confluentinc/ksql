package io.confluent.ksql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.serde.DataSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static io.confluent.ksql.EndToEndEngineTest.findTests;
import static io.confluent.ksql.EndToEndEngineTest.Query;
import static io.confluent.ksql.EndToEndEngineTest.Record;
import static io.confluent.ksql.EndToEndEngineTest.SerdeSupplier;
import static io.confluent.ksql.EndToEndEngineTest.StringSerdeSupplier;
import static io.confluent.ksql.EndToEndEngineTest.ValueSpecAvroSerdeSupplier;
import static io.confluent.ksql.EndToEndEngineTest.ValueSpecJsonSerdeSupplier;
import static io.confluent.ksql.EndToEndEngineTest.Topic;
import static io.confluent.ksql.EndToEndEngineTest.Window;

@RunWith(Parameterized.class)
public class QueryTranslationTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final String QUERY_VALIDATION_TEST_DIR = "query-validation-tests";

  private final String name;
  private final Query query;

  /**
   * @param name  - unused. Is just so the tests get named.
   * @param query - query to run.
   */
  public QueryTranslationTest(final String name, final Query query) {
    this.name = name;
    this.query = query;
  }

  @Test
  public void shouldBuildAndExecuteQueries() {
    EndToEndEngineTest.shouldBuildAndExecuteQuery(this.query);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() throws IOException {
    final List<String> testFiles = findTests(QUERY_VALIDATION_TEST_DIR);
    return testFiles.stream().flatMap(test -> {
      final String testPath = QUERY_VALIDATION_TEST_DIR + "/" + test;
      final JsonNode tests;
      try {
        tests = objectMapper.readTree(
            EndToEndEngineTest.class.getClassLoader().
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
          final List<Topic> topics = new LinkedList<>();
          topics.addAll(
              Arrays.asList(
                  new Topic("test_topic", null, new StringSerdeSupplier()),
                  new Topic("test_table", null, new StringSerdeSupplier())));
          if (query.has("topics")) {
            query.findValue("topics").forEach(
                topic -> topics.add(createTopicFromNode(topic))
            );
          }
          query.findValue("statements").elements()
              .forEachRemaining(statement -> statements.add(statement.asText()));
          query.findValue("inputs").elements()
              .forEachRemaining(input -> inputs.add(createRecordFromNode(topics, input)));
          query.findValue("outputs").elements()
              .forEachRemaining(output -> outputs.add(createRecordFromNode(topics, output)));
          queries.add(
              new Query(testPath, name, topics, inputs, outputs, statements));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
      return queries.stream()
          .map(query -> new Object[]{query.getName(), query});
    }).collect(Collectors.toCollection(ArrayList::new));
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
        final String schemaString = objectMapper.writeValueAsString(node);
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
