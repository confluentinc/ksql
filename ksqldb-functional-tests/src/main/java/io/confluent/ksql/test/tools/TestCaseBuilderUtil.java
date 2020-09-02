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

package io.confluent.ksql.test.tools;

import static com.google.common.io.Files.getNameWithoutExtension;

import com.google.common.collect.Streams;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.RegisterType;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.serde.SerdeOptionsFactory;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.topic.TopicFactory;
import io.confluent.ksql.util.KsqlConfig;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Util class for common code used by test case builders
 */
@SuppressWarnings("UnstableApiUsage")
public final class TestCaseBuilderUtil {

  private static final String FORMAT_REPLACE_ERROR =
      "To use {FORMAT} in your statements please set the 'format' test case element";

  private TestCaseBuilderUtil() {
  }

  public static String buildTestName(
      final Path originalFileName,
      final String testName,
      final Optional<String> explicitFormat
  ) {
    final String prefix = filePrefix(originalFileName.toString());

    final String pf = explicitFormat
        .map(f -> " - " + f)
        .orElse("");

    return prefix + testName + pf;
  }

  public static String extractSimpleTestName(
      final String originalFileName,
      final String testName
  ) {
    final String prefix = filePrefix(originalFileName);

    if (!testName.startsWith(prefix)) {
      throw new IllegalArgumentException("Not prefixed test name: " + testName);
    }

    return testName.substring(prefix.length());
  }

  public static List<String> buildStatements(
      final List<String> statements,
      final Optional<String> explicitFormat
  ) {
    final String format = explicitFormat.orElse(FORMAT_REPLACE_ERROR);

    return statements.stream()
        .map(stmt -> stmt.replace("{FORMAT}", format))
        .collect(Collectors.toList());
  }

  public static Collection<Topic> getAllTopics(
      final Collection<String> statements,
      final Collection<Topic> topics,
      final Collection<Record> outputs,
      final Collection<Record> inputs,
      final FunctionRegistry functionRegistry,
      final KsqlConfig ksqlConfig
  ) {
    final Map<String, Topic> allTopics = new HashMap<>();

    // Add all topics from topic nodes to the map:
    topics.forEach(topic -> allTopics.put(topic.getName(), topic));

    // Infer topics if not added already:
    final MutableMetaStore metaStore = new MetaStoreImpl(functionRegistry);
    for (String sql : statements) {
      final Topic topicFromStatement = createTopicFromStatement(sql, metaStore, ksqlConfig);
      if (topicFromStatement != null) {
        allTopics.putIfAbsent(topicFromStatement.getName(), topicFromStatement);
      }
    }

    // Get topics from inputs and outputs fields:
    Streams.concat(inputs.stream(), outputs.stream())
        .map(record -> new Topic(record.getTopicName(), Optional.empty()))
        .forEach(topic -> allTopics.putIfAbsent(topic.getName(), topic));

    return allTopics.values();
  }

  private static Topic createTopicFromStatement(
      final String sql,
      final MutableMetaStore metaStore,
      final KsqlConfig ksqlConfig
  ) {
    final KsqlParser parser = new DefaultKsqlParser();

    final Function<PreparedStatement<?>, Topic> extractTopic = (PreparedStatement<?> stmt) -> {
      final CreateSource statement = (CreateSource) stmt.getStatement();

      final KsqlTopic ksqlTopic = TopicFactory.create(statement.getProperties());

      final ValueFormat valueFormat = ksqlTopic.getValueFormat();
      final Optional<ParsedSchema> valueSchema;
      if (valueFormat.getFormat().supportsSchemaInference()) {
        final LogicalSchema logicalSchema = statement.getElements().toLogicalSchema();

        SerdeOptions serdeOptions;
        try {
          serdeOptions = SerdeOptionsFactory.buildForCreateStatement(
              logicalSchema,
              statement.getProperties().getValueFormat(),
              statement.getProperties().getSerdeOptions(),
              ksqlConfig
          );
        } catch (final Exception e) {
          // Catch block allows negative tests to fail in the correct place, later.
          serdeOptions = SerdeOptions.of();
        }

        valueSchema = logicalSchema.value().isEmpty()
            ? Optional.empty()
            : Optional.of(valueFormat.getFormat().toParsedSchema(
                logicalSchema.value(),
                serdeOptions,
                valueFormat.getFormatInfo()
            ));
      } else {
        valueSchema = Optional.empty();
      }

      final int partitions = statement.getProperties().getPartitions()
          .orElse(Topic.DEFAULT_PARTITIONS);

      final short rf = statement.getProperties().getReplicas()
          .orElse(Topic.DEFAULT_RF);

      return new Topic(ksqlTopic.getKafkaTopicName(), partitions, rf, valueSchema);
    };

    try {
      final List<ParsedStatement> parsed = parser.parse(sql);
      if (parsed.size() > 1) {
        throw new IllegalArgumentException("SQL contains more than one statement: " + sql);
      }

      final List<Topic> topics = new ArrayList<>();
      for (ParsedStatement stmt : parsed) {
        // in order to extract the topics, we may need to also register type statements
        if (stmt.getStatement().statement() instanceof SqlBaseParser.RegisterTypeContext) {
          final PreparedStatement<?> prepare = parser.prepare(stmt, metaStore);
          registerType(prepare, metaStore);
        }

        if (isCsOrCT(stmt)) {
          final PreparedStatement<?> prepare = parser.prepare(stmt, metaStore);
          topics.add(extractTopic.apply(prepare));
        }
      }

      return topics.isEmpty() ? null : topics.get(0);
    } catch (final Exception e) {
      // Statement won't parse: this will be detected/handled later.
      System.out.println("Error parsing statement (which may be expected): " + sql);
      e.printStackTrace(System.out);
      return null;
    }
  }

  private static void registerType(final PreparedStatement<?> prepare, final MetaStore metaStore) {
    if (prepare.getStatement() instanceof RegisterType) {
      final RegisterType statement = (RegisterType) prepare.getStatement();
      metaStore.registerType(statement.getName(), statement.getType().getSqlType());
    }
  }

  private static boolean isCsOrCT(final ParsedStatement stmt) {
    return stmt.getStatement().statement() instanceof SqlBaseParser.CreateStreamContext
        || stmt.getStatement().statement() instanceof SqlBaseParser.CreateTableContext;
  }

  private static String filePrefix(final String testPath) {
    return getNameWithoutExtension(testPath) + " - ";
  }

  private static class TestColumn implements SimpleColumn {

    private final ColumnName name;
    private final SqlType type;

    TestColumn(final ColumnName name, final SqlType type) {
      this.name = Objects.requireNonNull(name, "name");
      this.type = Objects.requireNonNull(type, "type");
    }

    @Override
    public ColumnName name() {
      return name;
    }

    @Override
    public SqlType type() {
      return type;
    }
  }
}
