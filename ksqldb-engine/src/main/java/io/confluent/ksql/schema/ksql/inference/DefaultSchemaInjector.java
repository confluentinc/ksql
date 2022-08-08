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
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.inference.TopicSchemaSupplier.SchemaAndId;
import io.confluent.ksql.schema.ksql.inference.TopicSchemaSupplier.SchemaResult;
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
 * An injector which injects the value columns into the supplied {@code statement}.
 *
 * <p>The value columns are only injected if:
 * <ul>
 * <li>The statement is a CT/CS.</li>
 * <li>The statement does not defined any value columns.</li>
 * <li>The format of the statement supports schema inference.</li>
 * </ul>
 *
 * <p>Any key columns present are passed through unchanged.
 *
 * <p>If any of the above are not true then the {@code statement} is returned unchanged.
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
    if (hasValueElements(statement)
        || !statement.getStatement().getProperties().getValueFormat().supportsSchemaInference()) {
      return Optional.empty();
    }

    final SchemaAndId valueSchema = getValueSchema(statement);
    final CreateSource withSchema = addSchemaFields(statement, valueSchema);
    final PreparedStatement<CreateSource> prepared = buildPreparedStatement(withSchema);
    final ConfiguredStatement<CreateSource> configured = ConfiguredStatement
        .of(prepared, statement.getConfigOverrides(), statement.getConfig());

    return Optional.of(configured);
  }

  private SchemaAndId getValueSchema(
      final ConfiguredStatement<CreateSource> statement
  ) {
    final String topicName = statement.getStatement().getProperties().getKafkaTopic();

    final SchemaResult result = statement.getStatement().getProperties().getSchemaId()
        .map(id -> schemaSupplier.getValueSchema(topicName, Optional.of(id)))
        .orElseGet(() -> schemaSupplier.getValueSchema(topicName, Optional.empty()));

    if (result.failureReason.isPresent()) {
      final Exception cause = result.failureReason.get();
      throw new KsqlStatementException(
          cause.getMessage(),
          statement.getMaskedStatementText(),
          cause);
    }

    return result.schemaAndId.get();
  }

  private static boolean hasValueElements(
      final ConfiguredStatement<CreateSource> statement
  ) {
    return statement.getStatement().getElements().stream()
        .anyMatch(e -> e.getNamespace().equals(Namespace.VALUE));
  }

  private static CreateSource addSchemaFields(
      final ConfiguredStatement<CreateSource> preparedStatement,
      final SchemaAndId schema
  ) {
    final TableElements elements = buildElements(schema.columns, preparedStatement);

    final CreateSource statement = preparedStatement.getStatement();
    final CreateSourceProperties properties = statement.getProperties();

    if (properties.getSchemaId().isPresent()) {
      return statement.copyWith(elements, properties);
    }
    return statement.copyWith(elements, properties.withSchemaId(schema.id));
  }

  private static TableElements buildElements(
      final List<? extends SimpleColumn> valueColumns,
      final ConfiguredStatement<CreateSource> preparedStatement
  ) {
    final List<TableElement> elements = new ArrayList<>();

    getKeyColumns(preparedStatement)
        .forEach(elements::add);

    valueColumns.stream()
        .map(col -> new TableElement(Namespace.VALUE, col.name(), new Type(col.type())))
        .forEach(elements::add);

    return TableElements.of(elements);
  }

  private static Stream<TableElement> getKeyColumns(
      final ConfiguredStatement<CreateSource> preparedStatement
  ) {
    return preparedStatement.getStatement().getElements().stream()
        .filter(e -> e.getNamespace().isKey());
  }

  private static PreparedStatement<CreateSource> buildPreparedStatement(
      final CreateSource stmt
  ) {
    return PreparedStatement.of(SqlFormatter.formatSql(stmt), stmt);
  }
}
