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

import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.properties.with.SourcePropertiesUtil;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.schema.ksql.inference.TopicSchemaSupplier.SchemaAndId;
import io.confluent.ksql.schema.ksql.inference.TopicSchemaSupplier.SchemaResult;
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
          statement.getMaskedStatementText(),
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

    if (hasKeyElements(statement) || !formatSupportsSchemaInference(keyFormat)) {
      return Optional.empty();
    }

    return Optional.of(getSchema(
        props.getKafkaTopic(),
        props.getKeySchemaId(),
        keyFormat,
        // until we support user-configuration of single key wrapping/unwrapping, we choose
        // to have key schema inference always result in an unwrapped key
        SerdeFeaturesFactory.buildKeyFeatures(FormatFactory.of(keyFormat), true),
        statement.getMaskedStatementText(),
        true
    ));
  }

  private Optional<SchemaAndId> getValueSchema(
      final ConfiguredStatement<CreateSource> statement
  ) {
    final CreateSourceProperties props = statement.getStatement().getProperties();
    final FormatInfo valueFormat = SourcePropertiesUtil.getValueFormat(props);

    if (hasValueElements(statement) || !formatSupportsSchemaInference(valueFormat)) {
      return Optional.empty();
    }

    return Optional.of(getSchema(
        props.getKafkaTopic(),
        props.getValueSchemaId(),
        valueFormat,
        props.getValueSerdeFeatures(),
        statement.getMaskedStatementText(),
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

  private static CreateSource addSchemaFields(
      final ConfiguredStatement<CreateSource> preparedStatement,
      final Optional<SchemaAndId> keySchema,
      final Optional<SchemaAndId> valueSchema
  ) {
    final TableElements elements = buildElements(preparedStatement, keySchema, valueSchema);

    final CreateSource statement = preparedStatement.getStatement();
    final CreateSourceProperties properties = statement.getProperties();

    final CreateSourceProperties withSchemaIds = properties.withSchemaIds(
        keySchema.map(s -> s.id),
        valueSchema.map(s -> s.id));
    return statement.copyWith(elements, withSchemaIds);
  }

  private static TableElements buildElements(
      final ConfiguredStatement<CreateSource> preparedStatement,
      final Optional<SchemaAndId> keySchema,
      final Optional<SchemaAndId> valueSchema
  ) {
    final List<TableElement> elements = new ArrayList<>();

    if (keySchema.isPresent()) {
      final Namespace namespace = getKeyNamespace(preparedStatement.getStatement());
      keySchema.get().columns.stream()
          .map(col -> new TableElement(namespace, col.name(), new Type(col.type())))
          .forEach(elements::add);
    } else {
      getKeyColumns(preparedStatement)
          .forEach(elements::add);
    }

    if (valueSchema.isPresent()) {
      valueSchema.get().columns.stream()
          .map(col -> new TableElement(Namespace.VALUE, col.name(), new Type(col.type())))
          .forEach(elements::add);
    } else {
      getValueColumns(preparedStatement)
          .forEach(elements::add);
    }

    return TableElements.of(elements);
  }

  private static Namespace getKeyNamespace(final CreateSource statement) {
    if (statement instanceof CreateStream) {
      return Namespace.KEY;
    } else if (statement instanceof CreateTable) {
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

  private static PreparedStatement<CreateSource> buildPreparedStatement(
      final CreateSource stmt
  ) {
    return PreparedStatement.of(SqlFormatter.formatSql(stmt), stmt);
  }
}
