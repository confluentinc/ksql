/*
 * Copyright 2019 Confluent Inc.
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


package io.confluent.ksql.test.commons;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import io.confluent.connect.avro.AvroData;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.schema.ksql.LogicalSchemas;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.StringUtil;
import java.nio.file.Path;
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

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
@SuppressWarnings("UnstableApiUsage")
@JsonIgnoreProperties(ignoreUnknown = true)
public class TestCaseNode {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  String name;
  private final List<String> formats;
  private final List<RecordNode> inputs;
  private final List<RecordNode> outputs;
  private final List<TopicNode> topics;
  private final List<String> statements;
  private final Map<String, Object> properties;
  private final Optional<ExpectedExceptionNode> expectedException;
  private final Optional<PostConditionsNode> postConditions;

  TestCaseNode(final TestCaseNode testCaseNode, final List<String> statements) {
    this.name = testCaseNode.name;
    this.formats = ImmutableList.copyOf(testCaseNode.formats);
    this.statements = ImmutableList.copyOf(statements);
    this.inputs = ImmutableList.copyOf(testCaseNode.inputs);
    this.outputs = ImmutableList.copyOf(testCaseNode.outputs);
    this.topics = ImmutableList.copyOf(testCaseNode.topics);
    this.properties = ImmutableMap.copyOf(testCaseNode.properties);
    this.expectedException = testCaseNode.expectedException;
    this.postConditions = testCaseNode.postConditions;
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity|NPathComplexity
  TestCaseNode(
      @JsonProperty("name") final String name,
      @JsonProperty("format") final List<String> formats,
      @JsonProperty("inputs") final List<RecordNode> inputs,
      @JsonProperty("outputs") final List<RecordNode> outputs,
      @JsonProperty("topics") final List<TopicNode> topics,
      @JsonProperty("statements") final List<String> statements,
      @JsonProperty("properties") final Map<String, Object> properties,
      @JsonProperty("expectedException") final ExpectedExceptionNode expectedException,
      @JsonProperty("post") final PostConditionsNode postConditions
  ) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity|NPathComplexity
    this.name = name == null ? "" : name;
    this.formats = formats == null ? ImmutableList.of() : ImmutableList.copyOf(formats);
    this.statements = statements == null ? ImmutableList.of() : ImmutableList.copyOf(statements);
    this.inputs = inputs == null ? ImmutableList.of() : ImmutableList.copyOf(inputs);
    this.outputs = outputs == null ? ImmutableList.of() : ImmutableList.copyOf(outputs);
    this.topics = topics == null ? ImmutableList.of() : ImmutableList.copyOf(topics);
    this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
    this.expectedException = Optional.ofNullable(expectedException);
    this.postConditions = Optional.ofNullable(postConditions);

    if (this.name.isEmpty()) {
      throw new MissingFieldException("name");
    }

    if (this.statements.isEmpty()) {
      throw new InvalidFieldException("statements", "was empty");
    }

    if (this.inputs.isEmpty() != this.outputs.isEmpty()) {
      throw new InvalidFieldException("inputs and outputs",
          "either both, or neither, field should be set");
    }

    if (!this.inputs.isEmpty() && this.expectedException.isPresent()) {
      throw new InvalidFieldException("inputs and expectedException",
          "can not both be set");
    }
  }

  List<TestCase> buildTests(final Path testPath, final FunctionRegistry functionRegistry) {
    try {
      return formats.isEmpty()
          ? Stream.of(createTest(
              "",
          testPath,
          functionRegistry)).collect(Collectors.toList())
          : formats.stream()
              .map(format -> createTest(
                  format,
                  testPath,
                  functionRegistry)).collect(Collectors.toList());
    } catch (final Exception e) {
      throw new AssertionError("Invalid test '" + name + "': " + e.getMessage(), e);
    }
  }

  private TestCase createTest(
      final String format,
      final Path testPath,
      final FunctionRegistry functionRegistry) {
    final String testName = buildTestName(testPath, name, format);

    try {
      final List<String> statements = buildStatements(format);

      final Optional<ExpectedException> ee = buildExpectedException(statements);

      final Map<String, Topic> topics = getTestCaseTopics(
          statements,
          ee.isPresent(),
          functionRegistry);

      final List<Record> inputRecords = inputs.stream()
          .map(node -> node.build(topics))
          .collect(Collectors.toList());

      final List<Record> outputRecords = outputs.stream()
          .map(node -> node.build(topics))
          .collect(Collectors.toList());

      final PostConditions post = postConditions
          .map(PostConditionsNode::build)
          .orElse(PostConditions.NONE);

      return new TestCase(
          testPath,
          testName,
          properties,
          topics.values(),
          inputRecords,
          outputRecords,
          statements,
          ee.orElseGet(ExpectedException::none),
          post
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
      final boolean expectsException,
      final FunctionRegistry functionRegistry
  ) {
    final Map<String, Topic> allTopics = new HashMap<>();

    // Add all topics from topic nodes to the map:
    topics.stream()
        .map(TopicNode::build)
        .forEach(topic -> allTopics.put(topic.getName(), topic));

    // Infer topics if not added already:
    statements.stream()
        .map(s -> TestCaseNode.createTopicFromStatement(functionRegistry, s))
        .filter(Objects::nonNull)
        .forEach(topic -> allTopics.putIfAbsent(topic.getName(), topic));

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

  static Topic createTopicFromStatement(
      final FunctionRegistry functionRegistry,
      final String sql) {
    final KsqlParser parser = new DefaultKsqlParser();
    final MetaStoreImpl metaStore = new MetaStoreImpl(functionRegistry);

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
      return new Topic(
          topicName,
          avroSchema,
          getSerdeSupplier(format),
          KsqlConstants.legacyDefaultSinkPartitionCount,
          KsqlConstants.legacyDefaultSinkReplicaCount);
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
    } catch (final Exception e) {
      // Statement won't parse: this will be detected/handled later.
      return null;
    }
  }

  static Schema addNames(final Schema schema) {
    final SchemaBuilder builder;
    switch (schema.type()) {
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

  static SerdeSupplier getSerdeSupplier(final String format) {
    switch (format.toUpperCase()) {
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

  @SuppressWarnings("UnstableApiUsage")
  static String buildTestName(
      final Path testPath,
      final String testName,
      final String postfix
  ) {
    final String fileName = com.google.common.io.Files.getNameWithoutExtension(testPath.toString());
    final String pf = postfix.isEmpty() ? "" : " - " + postfix;
    return fileName + " - " + testName + pf;
  }
}