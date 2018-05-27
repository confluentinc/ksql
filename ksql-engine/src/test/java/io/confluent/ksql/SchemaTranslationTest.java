package io.confluent.ksql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.serde.DataSource;
import org.apache.avro.Schema;
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

@RunWith(Parameterized.class)
public class SchemaTranslationTest extends EndToEndEngineTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final String SCHEMA_VALIDATION_TEST_DIR = "schema-validation-tests";
  private static final String TOPIC_NAME = "TEST_INPUT";
  private static final String OUTPUT_TOPIC_NAME = "TEST_OUTPUT";

  public SchemaTranslationTest(String name, Query query) {
    super(name, query);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() throws IOException {
    final List<String> testFiles = findTests(SCHEMA_VALIDATION_TEST_DIR);
    List<Object[]> testParams = new LinkedList<>();
    for (String filename : testFiles) {
      final JsonNode tests;
      try {
        tests = objectMapper.readTree(
            EndToEndEngineTest.class.getClassLoader().getResourceAsStream(
                SCHEMA_VALIDATION_TEST_DIR + "/" + filename));
      } catch (IOException e) {
        throw new RuntimeException("Unable to load test at path " + filename);
      }
      List<Query> query = loadTests(tests);
      testParams.addAll(
          query
              .stream()
              .map(q -> new Object[]{q.getName(), q})
              .collect(Collectors.toList())
      );
    }
    return testParams;
  }

  private static List<Query> loadTests(JsonNode node) {
    List<Query> tests = new ArrayList<>();
    node.get("tests").forEach(
        testNode -> tests.add(loadTest(testNode))
    );
    return tests;
  }

  @SuppressWarnings("unchecked")
  private static List<Record> generateInputRecords(
      final String topicName, final org.apache.avro.Schema avroSchema) {
    final Generator generator = new Generator(avroSchema, new Random());
    return IntStream.range(0, 3).mapToObj(
        i -> new Record(
            topicName,
            "test-key",
            generator.generate(),
            0,
            null,
            new AvroSerdeSupplier()
        )
    ).collect(Collectors.toList());
  }

  @SuppressWarnings("unchecked")
  private static List<Record> getOutputRecords(
      final String topicName, final List<Record> inputRecords,
      final org.apache.avro.Schema avroSchema) {
    return inputRecords.stream()
        .map(
            r -> new Record(
                topicName,
                "test-key",
                avroToValueSpec(r.value(), avroSchema, true),
                0,
                null,
                new ValueSpecAvroSerdeSupplier()
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
  private static List<Record> loadRecords(final String topicName, final JsonNode node) {
    final List<Record> records = new LinkedList<>();
    node.forEach(
        child -> records.add(
            new Record(
                topicName,
                "test-key",
                loadRecordSpec(child),
                0,
                null,
                new ValueSpecAvroSerdeSupplier()
            )
        )
    );
    return records;
  }

  private static Query loadTest(JsonNode node) {
    JsonNode schemaNode = node.get("schema");
    String schemaString;
    try {
      schemaString = new ObjectMapper().writeValueAsString(schemaNode);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
    org.apache.avro.Schema avroSchema = parser.parse(schemaString);

    List<Record> inputRecords;
    List<Record> outputRecords;
    if (node.has("input_records")) {
      inputRecords = loadRecords(TOPIC_NAME, node.get("input_records"));
      outputRecords = loadRecords(TOPIC_NAME, node.get("output_records"));
    } else {
      inputRecords = generateInputRecords(TOPIC_NAME, avroSchema);
      outputRecords = getOutputRecords(OUTPUT_TOPIC_NAME, inputRecords, avroSchema);
    }

    String ddlStatement =
        String.format(
            "CREATE STREAM %s WITH (KAFKA_TOPIC='%s', VALUE_FORMAT='AVRO');",
            TOPIC_NAME, TOPIC_NAME);
    String csasStatement = "CREATE STREAM TEST_OUTPUT AS SELECT ";
    csasStatement += avroSchema.getFields()
        .stream()
        .map(Schema.Field::name)
        .collect(Collectors.joining(", "));
    csasStatement += " FROM " + TOPIC_NAME + ";";

    return new Query(
        "",
        node.get("name").asText(),
        Collections.singletonList(new Topic(TOPIC_NAME, DataSource.AVRO_SERDE_NAME, avroSchema)),
        inputRecords,
        outputRecords,
        Arrays.asList(ddlStatement, csasStatement));
  }
}
