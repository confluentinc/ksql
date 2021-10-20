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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.execution.ddl.commands.CreateSourceCommand;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.properties.with.SourcePropertiesUtil;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.inference.TopicSchemaSupplier.SchemaAndId;
import io.confluent.ksql.schema.ksql.inference.TopicSchemaSupplier.SchemaResult;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.SerdeFeaturesFactory;
import io.confluent.ksql.services.SandboxedServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.util.ErrorMessageUtil;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.Pair;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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

  private final TopicSchemaSupplier schemaSupplier;
  private final KsqlExecutionContext executionContext;
  private final ServiceContext serviceContext;

  public DefaultSchemaInjector(
      final TopicSchemaSupplier schemaSupplier,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    this.schemaSupplier = Objects.requireNonNull(schemaSupplier, "schemaSupplier");
    this.executionContext = Objects.requireNonNull(executionContext, "executionContext");
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends Statement> ConfiguredStatement<T> inject(
      final ConfiguredStatement<T> statement
  ) {
    if (!(statement.getStatement() instanceof CreateSource)
        && !(statement.getStatement() instanceof CreateAsSelect)) {
      return statement;
    }

    try {
      if (statement.getStatement() instanceof CreateSource) {
        final ConfiguredStatement<CreateSource> createStatement =
            (ConfiguredStatement<CreateSource>) statement;
        return (ConfiguredStatement<T>) forCreateStatement(createStatement).orElse(createStatement);
      } else {
        final ConfiguredStatement<CreateAsSelect> createStatement =
            (ConfiguredStatement<CreateAsSelect>) statement;
        return (ConfiguredStatement<T>) forCreateAsStatement(createStatement).orElse(
            createStatement);
      }
    } catch (final KsqlStatementException e) {
      throw e;
    } catch (final KsqlException e) {
      throw new KsqlStatementException(
          ErrorMessageUtil.buildErrorMessage(e),
          statement.getStatementText(),
          e.getCause());
    }
  }

  private Optional<ConfiguredStatement<CreateAsSelect>> forCreateAsStatement(
      final ConfiguredStatement<CreateAsSelect> statement
  ) {
    final CreateAsSelect csStmt = statement.getStatement();
    final CreateSourceAsProperties properties = csStmt.getProperties();

    // Don't need to inject schema if no key schema id and value schema id
    if (!properties.getKeySchemaId().isPresent() && !properties.getValueSchemaId().isPresent()) {
      return Optional.empty();
    }

    final CreateSourceCommand createSourceCommand;
    try {
      final ServiceContext sandboxServiceContext = SandboxedServiceContext.create(serviceContext);
      createSourceCommand = (CreateSourceCommand)
          executionContext.createSandbox(sandboxServiceContext)
              .plan(sandboxServiceContext, statement)
              .getDdlCommand()
              .get();
    } catch (final Exception e) {
      throw new KsqlStatementException(
          "Could not determine output schema for query due to error: "
              + e.getMessage(), statement.getStatementText(), e);
    }

    final Optional<SchemaAndId> keySchema = getCreateAsKeySchema(statement, createSourceCommand);
    final Optional<SchemaAndId> valueSchema = getCreateAsValueSchema(statement,
        createSourceCommand);

    // Create schema and put into statement
    final CreateAsSelect withSchema = addSchemaFieldsForCreateAs(statement, keySchema,
        valueSchema);
    final PreparedStatement<CreateAsSelect> prepared = buildPreparedStatement(withSchema);
    final ConfiguredStatement<CreateAsSelect> configured = ConfiguredStatement
        .of(prepared, statement.getSessionConfig());

    return Optional.of(configured);
  }

  private Optional<SchemaAndId> getCreateAsKeySchema(
      final ConfiguredStatement<CreateAsSelect> statement,
      final CreateSourceCommand createSourceCommand
  ) {
    final CreateAsSelect csStmt = statement.getStatement();
    final CreateSourceAsProperties props = csStmt.getProperties();
    if (!props.getKeySchemaId().isPresent()) {
      return Optional.empty();
    }

    final FormatInfo keyFormat = createSourceCommand.getFormats().getKeyFormat();

    // until we support user-configuration of single key wrapping/unwrapping, we choose
    // to have key schema inference always result in an unwrapped key
    final SerdeFeatures serdeFeatures = SerdeFeaturesFactory.buildKeyFeatures(
        FormatFactory.of(keyFormat), true);

    validateSchemaInference(keyFormat, true);

    final SchemaAndId schemaAndId = getSchema(
        props.getKafkaTopic(),
        props.getKeySchemaId(),
        keyFormat,
        serdeFeatures,
        statement.getStatementText(),
        true
    );

    final List<Column> tableColumns = createSourceCommand.getSchema().key();
    checkColumnsCompatibility(props.getKeySchemaId(), tableColumns, schemaAndId.columns, true);

    return Optional.of(schemaAndId);
  }

  private Optional<SchemaAndId> getCreateAsValueSchema(
      final ConfiguredStatement<CreateAsSelect> statement,
      final CreateSourceCommand createSourceCommand
  ) {
    final CreateAsSelect csStmt = statement.getStatement();
    final CreateSourceAsProperties props = csStmt.getProperties();
    if (!props.getValueSchemaId().isPresent()) {
      return Optional.empty();
    }

    final FormatInfo valueFormat = createSourceCommand.getFormats().getValueFormat();
    validateSchemaInference(valueFormat, false);

    final SchemaAndId schemaAndId = getSchema(
        props.getKafkaTopic(),
        props.getValueSchemaId(),
        valueFormat,
        createSourceCommand.getFormats().getValueFeatures(),
        statement.getStatementText(),
        false
    );

    final List<Column> tableColumns = createSourceCommand.getSchema().value();
    checkColumnsCompatibility(props.getValueSchemaId(), tableColumns, schemaAndId.columns, false);

    return Optional.of(schemaAndId);
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
    final ConfiguredStatement<CreateSource> configured = ConfiguredStatement
        .of(prepared, statement.getSessionConfig());

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

    final SchemaAndId schemaAndId = getSchema(
        Optional.of(props.getKafkaTopic()),
        props.getKeySchemaId(),
        keyFormat,
        // until we support user-configuration of single key wrapping/unwrapping, we choose
        // to have key schema inference always result in an unwrapped key
        SerdeFeaturesFactory.buildKeyFeatures(FormatFactory.of(keyFormat), true),
        statement.getStatementText(),
        true
    );

    final List<Column> tableColumns =
        hasKeyElements(statement) ? statement.getStatement().getElements().toLogicalSchema().key()
            : Collections.emptyList();
    checkColumnsCompatibility(props.getKeySchemaId(), tableColumns, schemaAndId.columns, true);

    return Optional.of(schemaAndId);
  }

  private Optional<SchemaAndId> getValueSchema(
      final ConfiguredStatement<CreateSource> statement
  ) {
    final CreateSourceProperties props = statement.getStatement().getProperties();
    final FormatInfo valueFormat = SourcePropertiesUtil.getValueFormat(props);

    if (!shouldInferSchema(props.getValueSchemaId(), statement, valueFormat, false)) {
      return Optional.empty();
    }

    final SchemaAndId schemaAndId = getSchema(
        Optional.of(props.getKafkaTopic()),
        props.getValueSchemaId(),
        valueFormat,
        props.getValueSerdeFeatures(),
        statement.getStatementText(),
        false
    );

    final List<Column> tableColumns =
        hasValueElements(statement) ? statement.getStatement().getElements().toLogicalSchema()
            .value()
            : Collections.emptyList();
    checkColumnsCompatibility(props.getValueSchemaId(), tableColumns, schemaAndId.columns, false);

    return Optional.of(schemaAndId);
  }

  private SchemaAndId getSchema(
      final Optional<String> topicName,
      final Optional<Integer> schemaId,
      final FormatInfo expectedFormat,
      final SerdeFeatures serdeFeatures,
      final String statementText,
      final boolean isKey
  ) {
    final SchemaResult result =
        isKey ? schemaSupplier.getKeySchema(topicName, schemaId, expectedFormat, serdeFeatures)
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
    if (schemaId.isPresent()) {
      validateSchemaInference(formatInfo, isKey);
      return true;
    }

    final boolean hasTableElements =
        isKey ? hasKeyElements(statement) : hasValueElements(statement);

    return !hasTableElements && formatSupportsSchemaInference(formatInfo);
  }

  private static void validateSchemaInference(final FormatInfo formatInfo, final boolean isKey) {
    if (!formatSupportsSchemaInference(formatInfo)) {
      final String formatProp = isKey ? CommonCreateConfigs.KEY_FORMAT_PROPERTY
          : CommonCreateConfigs.VALUE_FORMAT_PROPERTY;
      final String schemaIdName =
          isKey ? CommonCreateConfigs.KEY_SCHEMA_ID : CommonCreateConfigs.VALUE_SCHEMA_ID;
      final String msg = String.format("%s should support schema inference when %s is provided. "
          + "Current format is %s.", formatProp, schemaIdName, formatInfo.getFormat());
      throw new KsqlException(msg);
    }
  }

  private static void checkColumnsCompatibility(
      final Optional<Integer> schemaId,
      final List<Column> tableColumns,
      final List<? extends SimpleColumn> connectColumns, final boolean isKey) {
    /*
     * Check inferred columns from schema id and provided columns compatibility. Conditions:
     *   1. Schema id is provided
     *   2. Table elements are provided
     *   3. Inferred columns should be superset of columns from table elements
     */
    if (!schemaId.isPresent()) {
      return;
    }

    if (tableColumns.isEmpty()) {
      return;
    }

    final Column.Namespace namespace = isKey ? Column.Namespace.KEY : Column.Namespace.VALUE;

    final List<Column> inferredColumns = IntStream.range(0, connectColumns.size()).mapToObj(
        i -> Column.of(
            connectColumns.get(i).name(),
            connectColumns.get(i).type(),
            namespace,
            i)
    ).collect(Collectors.toList());

    final ImmutableSet<Column> colA = ImmutableSet.copyOf(tableColumns);
    final ImmutableSet<Column> colB = ImmutableSet.copyOf(inferredColumns);

    final SetView<Column> difference = Sets.difference(colA, colB);
    if (!difference.isEmpty()) {
      throw new KsqlException("The following " + (isKey ? "key " : "value ")
          + "columns are changed, missing or reordered: "
          + difference + ". Schema from schema registry is " + inferredColumns);
    }
  }

  private static boolean hasKeyElements(
      final ConfiguredStatement<CreateSource> statement
  ) {
    return statement.getStatement().getElements().stream()
        .anyMatch(e -> e.getNamespace().isKey());
  }

  private static boolean hasValueElements(
      final ConfiguredStatement<CreateSource> statement
  ) {
    return statement.getStatement().getElements().stream()
        .anyMatch(e -> !e.getNamespace().isKey());
  }

  private static boolean formatSupportsSchemaInference(final FormatInfo format) {
    return FormatFactory.of(format).supportsFeature(SerdeFeature.SCHEMA_INFERENCE);
  }

  private static CreateAsSelect addSchemaFieldsForCreateAs(
      final ConfiguredStatement<CreateAsSelect> preparedStatement,
      final Optional<SchemaAndId> keySchema,
      final Optional<SchemaAndId> valueSchema
  ) {
    final TableElements elements = buildElements(preparedStatement, keySchema, valueSchema, null);

    final CreateAsSelect statement = preparedStatement.getStatement();
    final CreateSourceAsProperties properties = statement.getProperties();

    final CreateSourceAsProperties withSchemaIds = properties.withSchemaIds(
        keySchema.map(s -> s.id),
        valueSchema.map(s -> s.id));
    return statement.copyWith(elements, withSchemaIds);
  }

  private static CreateSource addSchemaFields(
      final ConfiguredStatement<CreateSource> preparedStatement,
      final Optional<SchemaAndId> keySchema,
      final Optional<SchemaAndId> valueSchema
  ) {
    final TableElements elements = buildElements(preparedStatement, keySchema, valueSchema,
        () -> Pair.of(getKeyColumns(preparedStatement), getValueColumns(preparedStatement)));

    final CreateSource statement = preparedStatement.getStatement();
    final CreateSourceProperties properties = statement.getProperties();

    final CreateSourceProperties withSchemaIds = properties.withSchemaIds(
        keySchema.map(s -> s.id),
        valueSchema.map(s -> s.id));
    return statement.copyWith(elements, withSchemaIds);
  }

  private static <T extends Statement> TableElements buildElements(
      final ConfiguredStatement<T> preparedStatement,
      final Optional<SchemaAndId> keySchema,
      final Optional<SchemaAndId> valueSchema,
      final Supplier<Pair<Stream<TableElement>, Stream<TableElement>>> kvColumnSupplier
  ) {
    final List<TableElement> elements = new ArrayList<>();

    if (keySchema.isPresent()) {
      final Namespace namespace = getKeyNamespace(preparedStatement.getStatement());
      keySchema.get().columns.stream()
          .map(col -> new TableElement(namespace, col.name(), new Type(col.type())))
          .forEach(elements::add);
    } else {
      if (Objects.nonNull(kvColumnSupplier)) {
        kvColumnSupplier.get().left.forEach(elements::add);
      }
    }

    if (valueSchema.isPresent()) {
      valueSchema.get().columns.stream()
          .map(col -> new TableElement(Namespace.VALUE, col.name(), new Type(col.type())))
          .forEach(elements::add);
    } else {
      if (Objects.nonNull(kvColumnSupplier)) {
        kvColumnSupplier.get().right.forEach(elements::add);
      }
    }

    return TableElements.of(elements);
  }

  private static Namespace getKeyNamespace(final Object statement) {
    if (statement instanceof CreateStream || statement instanceof CreateStreamAsSelect) {
      return Namespace.KEY;
    } else if (statement instanceof CreateTable || statement instanceof CreateTableAsSelect) {
      return Namespace.PRIMARY_KEY;
    } else {
      throw new IllegalArgumentException("Unrecognized statement type: " + statement);
    }
  }

  private static Stream<TableElement> getKeyColumns(
      final ConfiguredStatement<CreateSource> preparedStatement
  ) {
    return preparedStatement.getStatement().getElements().stream()
        .filter(e -> e.getNamespace().isKey());
  }

  private static Stream<TableElement> getValueColumns(
      final ConfiguredStatement<CreateSource> preparedStatement
  ) {
    return preparedStatement.getStatement().getElements().stream()
        .filter(e -> !e.getNamespace().isKey());
  }

  private static <T extends Statement> PreparedStatement<T> buildPreparedStatement(
      final T stmt
  ) {
    return PreparedStatement.of(SqlFormatter.formatSql(stmt), stmt);
  }
}
