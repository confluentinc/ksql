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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.format.DefaultFormatInjector;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.properties.with.SourcePropertiesUtil;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.RegisterType;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SchemaTranslator;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.SerdeFeaturesFactory;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.SourcePropertyInjector;
import io.confluent.ksql.tools.test.model.Topic;
import io.confluent.ksql.util.KsqlConfig;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
      final Optional<String> explicitFormat,
      final Optional<String> config
  ) {
    final String prefix = filePrefix(originalFileName.toString());

    final String pf = explicitFormat
        .map(f -> " - " + f)
        .orElse("");

    final String pc = config
        .map(f -> " - " + f)
        .orElse("");

    return prefix + testName + pf + pc;
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
    final Map<String, Topic> topicsByName = new HashMap<>();
    topics.forEach(topic -> topicsByName.put(topic.getName(), topic));

    // Infer topics if not added already:
    final MutableMetaStore metaStore = new MetaStoreImpl(functionRegistry);
    for (String sql : statements) {
      // Creates the `Topic` object when a schema is declared in the CREATE statement
      // and updates the `allTopics` with the schema and features found in the statement
      createTopicFromStatement(sql, metaStore, ksqlConfig).ifPresent(topicFromStatement -> {
        topicsByName.compute(topicFromStatement.getName(), (key, topic) -> {
          if (topic == null) {
            return topicFromStatement;
          } else {
            final Optional<ParsedSchema> keySchema = Optional.of(topic.getKeySchema())
                .filter(Optional::isPresent)
                .orElse(topicFromStatement.getKeySchema());

            final Optional<ParsedSchema> valueSchema = Optional.of(topic.getValueSchema())
                .filter(Optional::isPresent)
                .orElse(topicFromStatement.getValueSchema());

            topic = new Topic(
                topic.getName(),
                topic.getNumPartitions(),
                topic.getReplicas(),

                // key/value schema ID (if not empty) should be related to the key/value schema
                // already found in the 'Topic', not in the 'topicFromStatement'
                topic.getKeySchemaId(),
                topic.getValueSchemaId(),

                // Use the key/value schema built for the CREATE statement
                keySchema,
                valueSchema,
                topic.getKeySchemaReferences(),
                topic.getValueSchemaReferences(),

                // Use the serde features built for the CREATE statement
                topicFromStatement.getKeyFeatures(),
                topicFromStatement.getValueFeatures());

            return topic;
          }
        });
      });
    }

    // If the `Topic` information is not found or resolved directly from the statement, then
    // create a simple `Topic` using the input/outputs topic field
    Streams.concat(inputs.stream(), outputs.stream())
        .map(record -> new Topic(record.getTopicName(), Optional.empty(), Optional.empty()))
        .forEach(topic -> topicsByName.putIfAbsent(topic.getName(), topic));

    return topicsByName.values();
  }

  // CHECKSTYLE_RULES.OFF: NPathComplexity
  private static Optional<Topic> createTopicFromStatement(
      final String sql,
      final MutableMetaStore metaStore,
      final KsqlConfig ksqlConfig
  ) {
    // CHECKSTYLE_RULES.ON: NPathComplexity
    final KsqlParser parser = new DefaultKsqlParser();

    try {
      final List<ParsedStatement> parsed = parser.parse(sql);
      if (parsed.size() > 1) {
        throw new IllegalArgumentException("SQL contains more than one statement: " + sql);
      } else if (parsed.size() == 1) {
        final ParsedStatement stmt = parsed.get(0);

        // in order to extract the topics, we may need to also register type statements
        if (stmt.getStatement().statement() instanceof SqlBaseParser.RegisterTypeContext) {
          final PreparedStatement<?> prepare = parser.prepare(stmt, metaStore);
          registerType(prepare, metaStore);
          return Optional.empty();
        }

        if (isCsOrCT(stmt)) {
          final PreparedStatement<?> prepare = parser.prepare(stmt, metaStore);
          final ConfiguredStatement<?> configured =
              ConfiguredStatement.of(prepare, SessionConfig.of(ksqlConfig, Collections.emptyMap()));
          final ConfiguredStatement<?> withFormats = new DefaultFormatInjector().inject(configured);
          final ConfiguredStatement<?> withSourceProps =
              new SourcePropertyInjector().inject(withFormats);
          return createTopicFromCreateSource(sql, ksqlConfig, withSourceProps);
        }
      }

      return Optional.empty();
    } catch (final Exception e) {
      // Statement won't parse: this will be detected/handled later.
      System.out.println("Error parsing statement (which may be expected): " + sql);
      e.printStackTrace(System.out);
      return Optional.empty();
    }
  }

  private static Optional<Topic> createTopicFromCreateSource(
      final String sql,
      final KsqlConfig ksqlConfig,
      final ConfiguredStatement<?> stmt
  ) {
    final CreateSource statement = (CreateSource) stmt.getStatement();
    final CreateSourceProperties props = statement.getProperties();
    final TableElements tableElements = statement.getElements();

    if (Iterators.size(tableElements.iterator()) == 0) {
      return Optional.empty();
    }

    final LogicalSchema logicalSchema = tableElements.toLogicalSchema();

    final FormatInfo keyFormatInfo = SourcePropertiesUtil.getKeyFormat(
        props, statement.getName());
    final Format keyFormat = FormatFactory.fromName(keyFormatInfo.getFormat());
    final SerdeFeatures keySerdeFeats = buildKeyFeatures(
        keyFormat, logicalSchema);

    final Optional<ParsedSchema> keySchema =
        keyFormat.supportsFeature(SerdeFeature.SCHEMA_INFERENCE)
            ? buildSchema(sql, logicalSchema.key(), keyFormatInfo, keyFormat, keySerdeFeats)
            : Optional.empty();

    final FormatInfo valFormatInfo = SourcePropertiesUtil.getValueFormat(props);
    final Format valFormat = FormatFactory.fromName(valFormatInfo.getFormat());
    final SerdeFeatures valSerdeFeats = buildValueFeatures(
        ksqlConfig, props, valFormat, logicalSchema);

    final Optional<ParsedSchema> valueSchema =
        valFormat.supportsFeature(SerdeFeature.SCHEMA_INFERENCE)
            ? buildSchema(
            sql, logicalSchema.value(), valFormatInfo, valFormat, valSerdeFeats)
            : Optional.empty();

    final int partitions = props.getPartitions()
        .orElse(Topic.DEFAULT_PARTITIONS);

    final short rf = props.getReplicas()
        .orElse(Topic.DEFAULT_RF);

    return Optional.of(new Topic(
        props.getKafkaTopic(),
        partitions,
        rf,
        // key/value schemas IDs do not exist if schema is found on the statement
        Optional.empty(),
        Optional.empty(),
        keySchema,
        valueSchema,
        ImmutableList.of(),
        ImmutableList.of(),
        keySerdeFeats,
        valSerdeFeats));
  }

  private static Optional<ParsedSchema> buildSchema(
      final String sql,
      final List<Column> schema,
      final FormatInfo formatInfo,
      final Format format,
      final SerdeFeatures serdeFeatures
  ) {
    if (schema.isEmpty()) {
      return Optional.empty();
    }

    try {
      final SchemaTranslator translator = format
          .getSchemaTranslator(formatInfo.getProperties());

      return Optional.of(translator.toParsedSchema(PersistenceSchema.from(schema, serdeFeatures)
      ));
    } catch (final Exception e) {
      // Statement won't parse: this will be detected/handled later.
      System.out
          .println("Error getting schema translator statement (which may be expected): " + sql);
      e.printStackTrace(System.out);
      return Optional.empty();
    }
  }

  private static SerdeFeatures buildKeyFeatures(final Format format, final LogicalSchema schema) {
    try {
      return SerdeFeaturesFactory.buildKeyFeatures(
          schema,
          format
      );
    } catch (final Exception e) {
      // Catch block allows negative tests to fail in the correct place, i.e. later.
      return SerdeFeatures.of();
    }
  }

  private static SerdeFeatures buildValueFeatures(
      final KsqlConfig ksqlConfig,
      final CreateSourceProperties props,
      final Format valueFormat,
      final LogicalSchema logicalSchema
  ) {
    try {
      return SerdeFeaturesFactory.buildValueFeatures(
          logicalSchema,
          valueFormat,
          props.getValueSerdeFeatures(),
          ksqlConfig
      );
    } catch (final Exception e) {
      // Catch block allows negative tests to fail in the correct place, i.e. later.
      return SerdeFeatures.of();
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
}
