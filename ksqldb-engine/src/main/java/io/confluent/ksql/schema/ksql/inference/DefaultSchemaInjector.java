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

package io.confluent.ksql.schema.ksql.inference;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.properties.with.SourcePropertiesUtil;
import io.confluent.ksql.parser.tree.ColumnConstraints;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.schema.ksql.inference.TopicSchemaSupplier.SchemaAndId;
import io.confluent.ksql.schema.ksql.inference.TopicSchemaSupplier.SchemaResult;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.SerdeFeaturesFactory;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.util.ErrorMessageUtil;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * An injector which injects key and/or value columns into the supplied {@code statement}.
 *
 * <p>Key columns are only injected if:
 * <ul>
 * <li>The statement is a CT/CS.</li>
 * <li>The statement does not defined any key columns.</li>
 * <li>The key format of the statement supports schema inference.</li>
 * </ul>
 * And similarly for value columns.
 *
 * <p>If key and/or value columns are present, then they are passed through unchanged.
 *
 * <p>If the above conditions are met for neither key nor value columns, then the
 * {@code statement} is returned unchanged.
 */
public class DefaultSchemaInjector implements Injector {

  private static final ColumnConstraints KEY_CONSTRAINT =
      new ColumnConstraints.Builder().key().build();

  private static final ColumnConstraints PRIMARY_KEY_CONSTRAINT =
      new ColumnConstraints.Builder().primaryKey().build();

  private final TopicSchemaSupplier schemaSupplier;

  public DefaultSchemaInjector(final TopicSchemaSupplier schemaSupplier) {
    this.schemaSupplier = Objects.requireNonNull(schemaSupplier, "schemaSupplier");
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends Statement> ConfiguredStatement<T> inject(
      final ConfiguredStatement<T> statement
  ) {
    if (!(statement.getStatement() instanceof CreateSource)) {
      return statement;
    }

    final ConfiguredStatement<CreateSource> createStatement =
        (ConfiguredStatement<CreateSource>) statement;

    try {
      return (ConfiguredStatement<T>) forCreateStatement(createStatement).orElse(createStatement);
    } catch (final KsqlStatementException e) {
      throw e;
    } catch (final KsqlException e) {
      throw new KsqlStatementException(
          ErrorMessageUtil.buildErrorMessage(e),
          statement.getStatementText(),
          e.getCause());
    }
  }

  private Optional<ConfiguredStatement<CreateSource>> forCreateStatement(
      final ConfiguredStatement<CreateSource> statement
  ) {
    final Optional<SchemaAndId> keySchema = getKeySchema(statement);
    final Optional<SchemaAndId> valueSchema = getValueSchema(statement);
    if (!keySchema.isPresent() && !valueSchema.isPresent()) {
      return Optional.empty();
    }

    final CreateSource withSchema = addSchemaFields(statement, keySchema, valueSchema);
    final PreparedStatement<CreateSource> prepared = buildPreparedStatement(withSchema);

    final ImmutableMap.Builder<String, Object> overrideBuilder =
        ImmutableMap.builder();

    // Only store raw schema if schema id is provided by user
    if (withSchema.getProperties().getKeySchemaId().isPresent()) {
      keySchema.map(
          schemaAndId -> overrideBuilder.put(CommonCreateConfigs.KEY_SCHEMA_ID, schemaAndId));
    }
    if (withSchema.getProperties().getValueSchemaId().isPresent()) {
      valueSchema.map(
          schemaAndId -> overrideBuilder.put(CommonCreateConfigs.VALUE_SCHEMA_ID,
              schemaAndId));
    }
    final ConfiguredStatement<CreateSource> configured = ConfiguredStatement
        .of(prepared, statement.getSessionConfig().copyWith(overrideBuilder.build()));

    return Optional.of(configured);
  }

  private Optional<SchemaAndId> getKeySchema(
      final ConfiguredStatement<CreateSource> statement
  ) {
    final CreateSource csStmt = statement.getStatement();
    final CreateSourceProperties props = csStmt.getProperties();
    final FormatInfo keyFormat = SourcePropertiesUtil.getKeyFormat(props, csStmt.getName());

    if (!shouldInferSchema(props.getKeySchemaId(), statement, keyFormat, true)) {
      return Optional.empty();
    }

    return Optional.of(getSchema(
        props.getKafkaTopic(),
        props.getKeySchemaId(),
        keyFormat,
        // until we support user-configuration of single key wrapping/unwrapping, we choose
        // to have key schema inference always result in an unwrapped key
        SerdeFeaturesFactory.buildKeyFeatures(FormatFactory.of(keyFormat), true),
        statement.getStatementText(),
        true
    ));
  }

  private Optional<SchemaAndId> getValueSchema(
      final ConfiguredStatement<CreateSource> statement
  ) {
    final CreateSourceProperties props = statement.getStatement().getProperties();
    final FormatInfo valueFormat = SourcePropertiesUtil.getValueFormat(props);

    if (!shouldInferSchema(props.getValueSchemaId(), statement, valueFormat, false)) {
      return Optional.empty();
    }

    return Optional.of(getSchema(
        props.getKafkaTopic(),
        props.getValueSchemaId(),
        valueFormat,
        props.getValueSerdeFeatures(),
        statement.getStatementText(),
        false
    ));
  }

  private SchemaAndId getSchema(
      final String topicName,
      final Optional<Integer> schemaId,
      final FormatInfo expectedFormat,
      final SerdeFeatures serdeFeatures,
      final String statementText,
      final boolean isKey
  ) {
    final SchemaResult result = isKey
        ? schemaSupplier.getKeySchema(topicName, schemaId, expectedFormat, serdeFeatures)
        : schemaSupplier.getValueSchema(topicName, schemaId, expectedFormat, serdeFeatures);

    if (result.failureReason.isPresent()) {
      final Exception cause = result.failureReason.get();
      throw new KsqlStatementException(
          cause.getMessage(),
          statementText,
          cause);
    }

    return result.schemaAndId.get();
  }

  private static boolean shouldInferSchema(
      final Optional<Integer> schemaId,
      final ConfiguredStatement<CreateSource> statement,
      final FormatInfo formatInfo,
      final boolean isKey
  ) {
    /*
     * Conditions for schema inference:
     *   1. key_schema_id or value_schema_id property exist or
     *   2. Table elements doesn't exist and format support schema inference
     *
     * Do validation when schemaId presents, so we need to infer schema. Conditions to meet:
     *  1. If schema id is provided, format must support schema inference
     */

    final String formatProp = isKey ? CommonCreateConfigs.KEY_FORMAT_PROPERTY
        : CommonCreateConfigs.VALUE_FORMAT_PROPERTY;
    final String schemaIdName =
        isKey ? CommonCreateConfigs.KEY_SCHEMA_ID : CommonCreateConfigs.VALUE_SCHEMA_ID;
    final String formatPropMsg = String.format("%s should support schema inference when %s is provided. "
        + "Current format is %s.", formatProp, schemaIdName, formatInfo.getFormat());

    final Format format;
    try {
      format = FormatFactory.of(formatInfo);
    } catch (KsqlException e) {
      if (e.getMessage().contains("does not support the following configs: [schemaId]")) {
        throw new KsqlException(formatPropMsg);
      }
      throw e;
    }
    final boolean hasTableElements =
        isKey ? hasKeyElements(statement) : hasValueElements(statement);
    if (schemaId.isPresent()) {
      if (!formatSupportsSchemaInference(format)) {
        throw new KsqlException(formatPropMsg);
      }
      if (hasTableElements) {
        final String msg = "Table elements and " + schemaIdName + " cannot both exist for create "
            + "statement.";
        throw new KsqlException(msg);
      }
      return true;
    }
    return !hasTableElements && formatSupportsSchemaInference(format);
  }

  private static boolean hasKeyElements(
      final ConfiguredStatement<CreateSource> statement
  ) {
    return statement.getStatement().getElements().stream()
        .map(TableElement::getConstraints)
        .anyMatch(c -> c.isKey() || c.isPrimaryKey());
  }

  private static boolean hasValueElements(
      final ConfiguredStatement<CreateSource> statement
  ) {
    return statement.getStatement().getElements().stream()
        .map(TableElement::getConstraints)
        .anyMatch(e -> !e.isKey() && !e.isPrimaryKey() && !e.isHeaders());
  }

  private static boolean formatSupportsSchemaInference(final Format format) {
    return format.supportsFeature(SerdeFeature.SCHEMA_INFERENCE);
  }

  private static CreateSource addSchemaFields(
      final ConfiguredStatement<CreateSource> preparedStatement,
      final Optional<SchemaAndId> keySchema,
      final Optional<SchemaAndId> valueSchema
  ) {
    final TableElements elements = buildElements(preparedStatement, keySchema, valueSchema);

    final CreateSource statement = preparedStatement.getStatement();
    final CreateSourceProperties properties = statement.getProperties();

    final Optional<String> keySchemaName;
    final Optional<String> valueSchemaName;

    // Only populate key and value schema names when schema ids are explicitly provided
    if (properties.getKeySchemaId().isPresent() && keySchema.isPresent()) {
      keySchemaName = Optional.ofNullable(keySchema.get().rawSchema.name());
    } else {
      keySchemaName = Optional.empty();
    }
    if (properties.getValueSchemaId().isPresent() && valueSchema.isPresent()) {
      valueSchemaName = Optional.ofNullable(valueSchema.get().rawSchema.name());
    } else {
      valueSchemaName = Optional.empty();
    }
    final CreateSourceProperties newProperties = statement.getProperties().withKeyValueSchemaName(
        keySchemaName, valueSchemaName);
    return statement.copyWith(elements, newProperties);
  }

  private static TableElements buildElements(
      final ConfiguredStatement<CreateSource> preparedStatement,
      final Optional<SchemaAndId> keySchema,
      final Optional<SchemaAndId> valueSchema
  ) {
    final List<TableElement> elements = new ArrayList<>();

    if (keySchema.isPresent()) {
      final ColumnConstraints constraints = getKeyConstraints(preparedStatement.getStatement());
      keySchema.get().columns.stream()
          .map(col -> new TableElement(col.name(), new Type(col.type()), constraints))
          .forEach(elements::add);
    } else {
      getKeyColumns(preparedStatement)
          .forEach(elements::add);
    }

    if (valueSchema.isPresent()) {
      valueSchema.get().columns.stream()
          .map(col -> new TableElement(col.name(), new Type(col.type())))
          .forEach(elements::add);
    } else {
      getValueColumns(preparedStatement)
          .forEach(elements::add);
    }

    return TableElements.of(elements);
  }

  private static ColumnConstraints getKeyConstraints(final CreateSource statement) {
    if (statement instanceof CreateStream) {
      return KEY_CONSTRAINT;
    } else if (statement instanceof CreateTable) {
      return PRIMARY_KEY_CONSTRAINT;
    } else {
      throw new IllegalArgumentException("Unrecognized statement type: " + statement);
    }
  }

  private static Stream<TableElement> getKeyColumns(
      final ConfiguredStatement<CreateSource> preparedStatement
  ) {
    return preparedStatement.getStatement().getElements().stream()
        .filter(e -> e.getConstraints().isKey()
            || e.getConstraints().isPrimaryKey());
  }

  private static Stream<TableElement> getValueColumns(
      final ConfiguredStatement<CreateSource> preparedStatement
  ) {
    return preparedStatement.getStatement().getElements().stream()
        .filter(e -> !e.getConstraints().isKey()
            && !e.getConstraints().isPrimaryKey()
            && !e.getConstraints().isHeaders());
  }

  private static PreparedStatement<CreateSource> buildPreparedStatement(
      final CreateSource stmt
  ) {
    return PreparedStatement.of(SqlFormatter.formatSql(stmt), stmt);
  }
}
