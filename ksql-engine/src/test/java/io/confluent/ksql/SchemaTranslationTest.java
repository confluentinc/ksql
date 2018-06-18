package io.confluent.ksql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.confluent.avro.random.generator.Generator;

import static io.confluent.ksql.EndToEndEngineTestUtil.avroToValueSpec;
import static io.confluent.ksql.EndToEndEngineTestUtil.findTests;
import static io.confluent.ksql.EndToEndEngineTestUtil.AvroSerdeSupplier;
import static io.confluent.ksql.EndToEndEngineTestUtil.Query;
import static io.confluent.ksql.EndToEndEngineTestUtil.Record;
import static io.confluent.ksql.EndToEndEngineTestUtil.ValueSpecAvroSerdeSupplier;
import static io.confluent.ksql.EndToEndEngineTestUtil.Topic;


@RunWith(Parameterized.class)
public class SchemaTranslationTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final String SCHEMA_VALIDATION_TEST_DIR = "schema-validation-tests";
  private static final String TOPIC_NAME = "TEST_INPUT";
  private static final String OUTPUT_TOPIC_NAME = "TEST_OUTPUT";

  private final String name;
  private final Query query;

  public SchemaTranslationTest(final String name, final Query query) {
    this.name = name;
    this.query = query;
  }

  @Test
  public void shouldBuildAndExecuteQueries() {
    EndToEndEngineTestUtil.shouldBuildAndExecuteQuery(this.query);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() throws IOException {
    final List<String> testFiles = findTests(SCHEMA_VALIDATION_TEST_DIR);
    final List<Object[]> testParams = new LinkedList<>();
    for (String filename : testFiles) {
      final JsonNode tests;
      try {
        tests = objectMapper.readTree(
            EndToEndEngineTestUtil.class.getClassLoader().getResourceAsStream(
                SCHEMA_VALIDATION_TEST_DIR + "/" + filename));
      } catch (IOException e) {
        throw new RuntimeException("Unable to load test at path " + filename);
      }
      final List<Query> query = loadTests(tests);
      testParams.addAll(
          query
              .stream()
              .map(q -> new Object[]{q.getName(), q})
              .collect(Collectors.toList())
      );
    }
    return testParams;
  }

  private static List<Query> loadTests(final JsonNode node) {
    final List<Query> tests = new ArrayList<>();
    node.get("tests").forEach(
        testNode -> tests.add(loadTest(testNode))
    );
    return tests;
  }

  @SuppressWarnings("unchecked")
  private static List<Record> generateInputRecords(
      final Topic topic, final org.apache.avro.Schema avroSchema) {
    final Generator generator = new Generator(avroSchema, new Random());
    return IntStream.range(0, 3).mapToObj(
        i -> new Record(
            topic,
            "test-key",
            generator.generate(),
            0,
            null
        )
    ).collect(Collectors.toList());
  }

  @SuppressWarnings("unchecked")
  private static List<Record> getOutputRecords(
      final Topic topic, final List<Record> inputRecords,
      final org.apache.avro.Schema avroSchema) {
    return inputRecords.stream()
        .map(
            r -> new Record(
                topic,
                "test-key",
                avroToValueSpec(r.value(), avroSchema, true),
                0,
                null
            ))
        .collect(Collectors.toList());

  }

  private static Object loadRecordSpec(final JsonNode node) {
    try {
      return objectMapper.readValue(
          objectMapper.writeValueAsString(node), Map.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private static List<Record> loadRecords(final Topic topic, final JsonNode node) {
    final List<Record> records = new LinkedList<>();
    node.forEach(
        child -> records.add(
            new Record(
                topic,
                "test-key",
                loadRecordSpec(child),
                0,
                null
            )
        )
    );
    return records;
  }

  private static Query loadTest(final JsonNode node) {
    final JsonNode schemaNode = node.get("schema");
    final String schemaString;
    try {
      schemaString = new ObjectMapper().writeValueAsString(schemaNode);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    final org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
    final org.apache.avro.Schema avroSchema = parser.parse(schemaString);

    final Topic srcTopic;
    final Topic outputTopic
        = new Topic(OUTPUT_TOPIC_NAME, null, new ValueSpecAvroSerdeSupplier());
    final List<Record> inputRecords;
    final List<Record> outputRecords;
    if (node.has("input_records")) {
      srcTopic = new Topic(TOPIC_NAME, avroSchema, new ValueSpecAvroSerdeSupplier());
      inputRecords = loadRecords(srcTopic, node.get("input_records"));
      outputRecords = loadRecords(outputTopic, node.get("output_records"));
    } else {
      srcTopic = new Topic(TOPIC_NAME, avroSchema, new AvroSerdeSupplier());
      inputRecords = generateInputRecords(srcTopic, avroSchema);
      outputRecords = getOutputRecords(outputTopic, inputRecords, avroSchema);
    }

    final String ddlStatement =
        String.format(
            "CREATE STREAM %s WITH (KAFKA_TOPIC='%s', VALUE_FORMAT='AVRO');",
            TOPIC_NAME, TOPIC_NAME);
    final String csasStatement = avroSchema.getFields()
        .stream()
        .map(Schema.Field::name)
        .collect(
            Collectors.joining(
                ", ",
                "CREATE STREAM TEST_OUTPUT AS SELECT ",
                " FROM " + TOPIC_NAME + ";")
        );

    return new Query(
        "",
        node.get("name").asText(),
        Collections.emptyMap(),
        ImmutableList.of(srcTopic, outputTopic),
        inputRecords,
        outputRecords,
        Arrays.asList(ddlStatement, csasStatement));
  }
}
