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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import io.confluent.connect.avro.AvroData;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.test.model.RecordNode;
import io.confluent.ksql.test.model.TopicNode;
import io.confluent.ksql.test.model.WindowData;
import io.confluent.ksql.test.model.WindowData.Type;
import io.confluent.ksql.test.serde.SerdeSupplier;
import io.confluent.ksql.test.tools.exceptions.InvalidFieldException;
import io.confluent.ksql.test.utils.SerdeUtil;
import io.confluent.ksql.topic.TopicFactory;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConstants;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

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
      final Path testPath,
      final String testName,
      final Optional<String> explicitFormat
  ) {
    final String fileName = com.google.common.io.Files.getNameWithoutExtension(testPath.toString());

    final String pf = explicitFormat
        .map(f -> " - " + f)
        .orElse("");

    return fileName + " - " + testName + pf;
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

  public static Map<String, Topic> getTopicsByName(
      final List<String> statements,
      final List<TopicNode> topics,
      final List<RecordNode> outputs,
      final List<RecordNode> inputs,
      final Optional<String> defaultFormat,
      final boolean expectsException,
      final FunctionRegistry functionRegistry
  ) {
    final Map<String, Topic> allTopics = new HashMap<>();

    // Add all topics from topic nodes to the map:
    topics.stream()
        .map(node -> node.build(defaultFormat))
        .forEach(topic -> allTopics.put(topic.getName(), topic));

    // Infer topics if not added already:
    statements.stream()
        .map(sql -> createTopicFromStatement(sql, functionRegistry))
        .filter(Objects::nonNull)
        .forEach(topic -> allTopics.putIfAbsent(topic.getName(), topic));

    if (allTopics.isEmpty()) {
      if (expectsException) {
        return ImmutableMap.of();
      }
      throw new InvalidFieldException("statements/topics", "The test does not define any topics");
    }

    final SerdeSupplier<?> defaultValueSerdeSupplier =
        allTopics.values().iterator().next().getValueSerdeSupplier();

    // Get topics from inputs and outputs fields:
    Streams.concat(inputs.stream(), outputs.stream())
        .map(recordNode -> new Topic(
            recordNode.topicName(),
            Optional.empty(),
            getKeySedeSupplier(recordNode.getWindow()),
            defaultValueSerdeSupplier,
            4,
            1,
            Optional.empty()))
        .forEach(topic -> allTopics.putIfAbsent(topic.getName(), topic));

    return allTopics;
  }

  private static Topic createTopicFromStatement(
      final String sql,
      final FunctionRegistry functionRegistry
  ) {
    final KsqlParser parser = new DefaultKsqlParser();
    final MetaStoreImpl metaStore = new MetaStoreImpl(functionRegistry);

    final Predicate<ParsedStatement> onlyCSandCT = stmt ->
        stmt.getStatement().statement() instanceof SqlBaseParser.CreateStreamContext
            || stmt.getStatement().statement() instanceof SqlBaseParser.CreateTableContext;

    final Function<PreparedStatement<?>, Topic> extractTopic = (PreparedStatement<?> stmt) -> {
      final CreateSource statement = (CreateSource) stmt.getStatement();

      final KsqlTopic ksqlTopic = TopicFactory.create(statement.getProperties());

      final KeyFormat keyFormat = ksqlTopic.getKeyFormat();

      final Supplier<LogicalSchema> logicalSchemaSupplier =
          statement.getElements()::toLogicalSchema;

      final SerdeSupplier<?> keySerdeSupplier =
          SerdeUtil.getKeySerdeSupplier(keyFormat, logicalSchemaSupplier);

      final ValueFormat valueFormat = ksqlTopic.getValueFormat();
      final Optional<org.apache.avro.Schema> avroSchema;
      if (valueFormat.getFormat() == Format.AVRO) {
        // add avro schema
        final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        statement.getElements().forEach(e -> schemaBuilder.field(
            e.getName(),
            SchemaConverters.sqlToConnectConverter().toConnectSchema(e.getType().getSqlType()))
        );
        avroSchema = Optional.of(new AvroData(1)
            .fromConnectSchema(addNames(schemaBuilder.build())));
      } else {
        avroSchema = Optional.empty();
      }

      final SerdeSupplier<?> valueSerdeSupplier = SerdeUtil.getSerdeSupplier(
          valueFormat.getFormat(),
          logicalSchemaSupplier
      );

      return new Topic(
          ksqlTopic.getKafkaTopicName(),
          avroSchema,
          keySerdeSupplier,
          valueSerdeSupplier,
          KsqlConstants.legacyDefaultSinkPartitionCount,
          KsqlConstants.legacyDefaultSinkReplicaCount,
          Optional.empty());
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
      System.out.println("Error parsing statement (which may be expected): " + sql);
      e.printStackTrace(System.out);
      return null;
    }
  }

  private static SerdeSupplier<?> getKeySedeSupplier(final Optional<WindowData> windowDataInfo) {
    if (windowDataInfo.isPresent()) {
      final WindowData windowData = windowDataInfo.get();
      final WindowType windowType = WindowType.of((windowData.type == Type.SESSION)
          ? WindowType.SESSION.name()
          : WindowType.TUMBLING.name());
      final KeyFormat windowKeyFormat = KeyFormat.windowed(
          Format.KAFKA,
          Optional.empty(),
          WindowInfo.of(
              windowType,
              windowType == WindowType.SESSION
                  ? Optional.empty() : Optional.of(Duration.ofMillis(windowData.size())))
      );
      return SerdeUtil.getKeySerdeSupplier(windowKeyFormat, () -> LogicalSchema.builder().build());
    }
    return SerdeUtil.getKeySerdeSupplier(
        KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
        () -> LogicalSchema.builder().build());
  }

  private static Schema addNames(final Schema schema) {
    final SchemaBuilder builder;
    switch (schema.type()) {
      case BYTES:
        DecimalUtil.requireDecimal(schema);
        builder = DecimalUtil.builder(schema);
        break;
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
}
