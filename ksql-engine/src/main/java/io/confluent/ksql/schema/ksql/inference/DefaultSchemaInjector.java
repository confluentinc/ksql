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

import com.google.common.collect.Iterables;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SchemaParser;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter.Option;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.inference.TopicSchemaSupplier.SchemaAndId;
import io.confluent.ksql.schema.ksql.inference.TopicSchemaSupplier.SchemaResult;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.ParserUtil;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;

/**
 * An injector which injects the schema into the supplied {@code statement}.
 *
 * <p>The schema is only injected if:
 * <ul>
 * <li>The statement is a CT/CS.</li>
 * <li>The statement does not defined a schema.</li>
 * <li>The format of the statement supports schema inference.</li>
 * </ul>
 *
 * <p>If any of the above are not true then the {@code statement} is returned unchanged.
 */
public class DefaultSchemaInjector implements Injector {

  private static final SqlSchemaFormatter FORMATTER = new SqlSchemaFormatter(
      ParserUtil::isReservedIdentifier, Option.AS_COLUMN_LIST);

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

    return (ConfiguredStatement<T>) forCreateStatement(createStatement).orElse(createStatement);
  }

  private Optional<ConfiguredStatement<CreateSource>> forCreateStatement(
      final ConfiguredStatement<CreateSource> statement
  ) {
    if (hasElements(statement)
        || statement.getStatement().getProperties().getValueFormat() != Format.AVRO) {
      return Optional.empty();
    }

    final SchemaAndId valueSchema = getValueSchema(statement);
    final CreateSource withSchema = addSchemaFields(statement, valueSchema);
    final PreparedStatement<CreateSource> prepared =
        buildPreparedStatement(withSchema);
    return Optional.of(ConfiguredStatement.of(
        prepared, statement.getOverrides(), statement.getConfig()));
  }

  private SchemaAndId getValueSchema(
      final ConfiguredStatement<CreateSource> statement
  ) {
    final String topicName = statement.getStatement().getProperties().getKafkaTopic();

    final SchemaResult result = statement.getStatement().getProperties().getAvroSchemaId()
        .map(id -> schemaSupplier.getValueSchema(topicName, Optional.of(id)))
        .orElseGet(() -> schemaSupplier.getValueSchema(topicName, Optional.empty()));

    if (result.failureReason.isPresent()) {
      final Exception cause = result.failureReason.get();
      throw new KsqlStatementException(
          cause.getMessage(),
          statement.getStatementText(),
          cause);
    }

    return result.schemaAndId.get();
  }

  private static boolean hasElements(
      final ConfiguredStatement<CreateSource> statement
  ) {
    return !Iterables.isEmpty(statement.getStatement().getElements());
  }

  private static CreateSource addSchemaFields(
      final ConfiguredStatement<CreateSource> preparedStatement,
      final SchemaAndId schema
  ) {
    final TableElements elements = buildElements(schema.schema, preparedStatement);

    final CreateSource statement = preparedStatement.getStatement();
    final CreateSourceProperties properties = statement.getProperties();

    if (properties.getAvroSchemaId().isPresent()) {
      return statement.copyWith(elements, properties);
    }
    return statement.copyWith(elements, properties.withSchemaId(schema.id));
  }

  private static TableElements buildElements(
      final Schema schema,
      final ConfiguredStatement<CreateSource> preparedStatement
  ) {
    try {
      throwOnInvalidSchema(schema);
      return SchemaParser.parse(FORMATTER.format(schema));
    } catch (final Exception e) {
      throw new KsqlStatementException(
          "Failed to convert schema to KSQL model: " + e.getMessage(),
          preparedStatement.getStatementText(),
          e);
    }
  }

  private static void throwOnInvalidSchema(final Schema schema) {
    SchemaConverters.connectToSqlConverter().toSqlType(schema);
  }

  private static PreparedStatement<CreateSource> buildPreparedStatement(
      final CreateSource stmt
  ) {
    return PreparedStatement.of(SqlFormatter.formatSql(stmt), stmt);
  }
}
