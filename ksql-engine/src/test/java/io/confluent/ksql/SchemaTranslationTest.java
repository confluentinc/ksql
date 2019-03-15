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

import static io.confluent.ksql.EndToEndEngineTestUtil.AvroSerdeSupplier;
import static io.confluent.ksql.EndToEndEngineTestUtil.Record;
import static io.confluent.ksql.EndToEndEngineTestUtil.TestCase;
import static io.confluent.ksql.EndToEndEngineTestUtil.Topic;
import static io.confluent.ksql.EndToEndEngineTestUtil.ValueSpecAvroSerdeSupplier;
import static io.confluent.ksql.EndToEndEngineTestUtil.avroToValueSpec;
import static io.confluent.ksql.EndToEndEngineTestUtil.buildAvroSchema;
import static io.confluent.ksql.EndToEndEngineTestUtil.buildTestName;
import static io.confluent.ksql.EndToEndEngineTestUtil.findTestCases;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import io.confluent.avro.random.generator.Generator;
import io.confluent.ksql.EndToEndEngineTestUtil.MissingFieldException;
import io.confluent.ksql.EndToEndEngineTestUtil.TestFile;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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

  private final TestCase testCase;
  private static final Topic OUTPUT_TOPIC = new Topic(OUTPUT_TOPIC_NAME, Optional.empty(),
      new ValueSpecAvroSerdeSupplier(), 4, 1);

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
    final List<String> testFiles = EndToEndEngineTestUtil.getTestFilesParam();

    return findTestCases(SCHEMA_VALIDATION_TEST_DIR, testFiles, SttTestFile.class)
        .map(test -> new Object[]{test.getName(), test})
        .collect(Collectors.toList());
  }

  private static List<Record> generateInputRecords(
      final Topic topic, final org.apache.avro.Schema avroSchema) {
    final Generator generator = new Generator(avroSchema, new Random());
    return IntStream.range(0, 100).mapToObj(
        i -> new Record(
            topic,
            "test-key",
            generator.generate(),
            0,
            null
        )
    ).collect(Collectors.toList());
  }

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

  @JsonIgnoreProperties(ignoreUnknown = true)
  static class SttTestFile implements TestFile<TestCase> {

    private final List<TestCaseNode> tests;

    SttTestFile(@JsonProperty("tests") final List<TestCaseNode> tests) {
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
  static class TestCaseNode {

    private final String name;
    private final JsonNode schema;

    TestCaseNode(
        @JsonProperty("name") final String name,
        @JsonProperty("schema") final JsonNode schema
    ) {
      this.name = name == null ? "" : name;
      this.schema = requireNonNull(schema, "schema");
    }

    Stream<TestCase> buildTests(final Path testPath) {
      if (name.isEmpty()) {
        throw new MissingFieldException("name");
      }

      final String testName = buildTestName(testPath, name, "");

      try {
        final Schema avroSchema = buildAvroSchema(schema)
            .orElseThrow(() -> new MissingFieldException("schema"));

        final Topic srcTopic = new Topic(
            TOPIC_NAME,
            Optional.of(avroSchema),
            new AvroSerdeSupplier(),
            1,
            1
        );

        final List<Record> inputRecords = generateInputRecords(srcTopic, avroSchema);
        final List<Record> outputRecords = getOutputRecords(OUTPUT_TOPIC, inputRecords, avroSchema);

        final String csasStatement = avroSchema.getFields()
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
            Collections.emptyMap(),
            ImmutableList.of(srcTopic, OUTPUT_TOPIC),
            inputRecords,
            outputRecords,
            ImmutableList.of(DDL_STATEMENT, csasStatement),
            EndToEndEngineTestUtil.ExpectedException.none()));
      } catch (final Exception e) {
        throw new AssertionError(testName + ": Invalid test. " + e.getMessage(), e);
      }
    }
  }
}
