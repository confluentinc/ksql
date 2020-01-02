package io.confluent.ksql.test;

import static io.confluent.ksql.test.EndToEndEngineTestUtil.avroToJson;
import static io.confluent.ksql.test.EndToEndEngineTestUtil.avroToValueSpec;
import static io.confluent.ksql.test.EndToEndEngineTestUtil.buildAvroSchema;
import static io.confluent.ksql.test.EndToEndEngineTestUtil.buildTestName;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import io.confluent.avro.random.generator.Generator;
import io.confluent.ksql.test.loader.JsonTestLoader;
import io.confluent.ksql.test.loader.TestFile;
import io.confluent.ksql.test.tools.Record;
import io.confluent.ksql.test.tools.TestCase;
import io.confluent.ksql.test.tools.Topic;
import io.confluent.ksql.test.tools.VersionBounds;
import io.confluent.ksql.test.tools.conditions.PostConditions;
import io.confluent.ksql.test.tools.exceptions.MissingFieldException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


@RunWith(Parameterized.class)
public class SchemaTranslationTest {

  private static final Path SCHEMA_VALIDATION_TEST_DIR = Paths.get("schema-validation-tests");
  private static final String TOPIC_NAME = "TEST_INPUT";
  private static final String OUTPUT_TOPIC_NAME = "TEST_OUTPUT";
  private static final String DDL_STATEMENT = "CREATE STREAM " + TOPIC_NAME
      + " WITH (KAFKA_TOPIC='" + TOPIC_NAME + "', VALUE_FORMAT='AVRO');";

  private static final Topic OUTPUT_TOPIC = new Topic(
      OUTPUT_TOPIC_NAME,
      1,
      1,
      Optional.empty()
  );

  private final TestCase testCase;

  @SuppressWarnings("unused")
  public SchemaTranslationTest(final String name, final TestCase testCase) {
    this.testCase = testCase;
  }

  @Test
  public void shouldBuildAndExecuteQueries() {
    EndToEndEngineTestUtil.shouldBuildAndExecuteQuery(testCase);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return JsonTestLoader.of(SCHEMA_VALIDATION_TEST_DIR, SttTestFile.class)
        .load()
        .map(test -> new Object[]{test.getName(), test})
        .collect(Collectors.toList());
  }

  private static List<Record> generateInputRecords(
      final Topic topic, final org.apache.avro.Schema avroSchema) {
    final Generator generator = new Generator(avroSchema, new Random());
    final List<Record> list = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      final Object avro = generator.generate();
      final JsonNode spec = avroToJson(avro, avroSchema, true);
      final Record record = new Record(
          topic,
          "test-key",
          avroToValueSpec(avro, avroSchema, true),
          spec,
          Optional.of(0L),
          null
      );
      list.add(record);
    }
    return list;
  }

  private static List<Record> getOutputRecords(final Topic topic, final List<Record> inputRecords) {
    return inputRecords.stream()
        .map(
            r -> new Record(
                topic,
                "test-key",
                r.value(),
                r.getJsonValue().orElse(null),
                Optional.of(0L),
                null
            ))
        .collect(Collectors.toList());

  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static class SttTestFile implements TestFile<TestCase> {

    private final List<SttCaseNode> tests;

    SttTestFile(@JsonProperty("tests") final List<SttCaseNode> tests) {
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

  @JsonIgnoreProperties(ignoreUnknown = true)
  static class SttCaseNode {

    private final String name;
    private final Schema schema;

    SttCaseNode(
        @JsonProperty("name") final String name,
        @JsonProperty("schema") final JsonNode schema
    ) {
      this.name = name == null ? "" : name;
      this.schema = buildAvroSchema(requireNonNull(schema, "schema"))
          .orElseThrow(() -> new MissingFieldException("schema"));

      if (this.name.isEmpty()) {
        throw new MissingFieldException("name");
      }
    }

    Stream<TestCase> buildTests(final Path testPath) {

      final String testName = buildTestName(testPath, name, "");

      try {
        final Topic srcTopic = new Topic(
            TOPIC_NAME,
            1,
            1,
            Optional.of(schema)
        );

        final List<Record> inputRecords = generateInputRecords(srcTopic, schema);
        final List<Record> outputRecords = getOutputRecords(OUTPUT_TOPIC, inputRecords);

        final String csasStatement = schema.getFields()
            .stream()
            .map(Schema.Field::name)
            .collect(
                Collectors.joining(
                    ", ",
                    "CREATE STREAM " + OUTPUT_TOPIC_NAME + " AS SELECT ",
                    " FROM " + TOPIC_NAME + ";")
            );

        return Stream.of(new TestCase(
            testPath,
            name,
            VersionBounds.allVersions(),
            Collections.emptyMap(),
            ImmutableList.of(srcTopic, OUTPUT_TOPIC),
            inputRecords,
            outputRecords,
            ImmutableList.of(DDL_STATEMENT, csasStatement),
            Optional.empty(),
            PostConditions.NONE
        ));
      } catch (final Exception e) {
        throw new AssertionError(testName + ": Invalid test. " + e.getMessage(), e);
      }
    }
  }
}
