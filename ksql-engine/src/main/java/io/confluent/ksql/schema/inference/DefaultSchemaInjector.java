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

package io.confluent.ksql.schema.inference;

import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.schema.inference.TopicSchemaSupplier.SchemaAndId;
import io.confluent.ksql.schema.inference.TopicSchemaSupplier.SchemaResult;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlStatementException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;

public class DefaultSchemaInjector implements SchemaInjector {

  private final TopicSchemaSupplier schemaSupplier;

  public DefaultSchemaInjector(final TopicSchemaSupplier schemaSupplier) {
    this.schemaSupplier = Objects.requireNonNull(schemaSupplier, "schemaSupplier");
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends Statement> PreparedStatement<T> forStatement(
      final PreparedStatement<T> statement
  ) {
    if (!(statement.getStatement() instanceof AbstractStreamCreateStatement)) {
      return statement;
    }

    return forCreateStatement((PreparedStatement<AbstractStreamCreateStatement>) statement)
        .map(DefaultSchemaInjector::buildPreparedStatement)
        .orElse(statement);
  }

  private Optional<AbstractStreamCreateStatement> forCreateStatement(
      final PreparedStatement<AbstractStreamCreateStatement> statement
  ) {
    if (hasElements(statement) || isUnsupportedFormat(statement)) {
      return Optional.empty();
    }

    final SchemaAndId valueSchema = getValueSchema(statement);

    return Optional.of(addSchemaFields(statement, valueSchema));
  }

  @SuppressWarnings("ConstantConditions") // result is union.
  private SchemaAndId getValueSchema(
      final PreparedStatement<AbstractStreamCreateStatement> statement
  ) {
    final String topicName = getKafkaTopicName(statement);

    final SchemaResult result = getSchemaId(statement)
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
      final PreparedStatement<AbstractStreamCreateStatement> statement
  ) {
    return !statement.getStatement().getElements().isEmpty();
  }

  private static boolean isUnsupportedFormat(
      final PreparedStatement<AbstractStreamCreateStatement> statement
  ) {
    final String valueFormat =
        getRequiredProperty(statement, DdlConfig.VALUE_FORMAT_PROPERTY);

    return !valueFormat.equalsIgnoreCase(DataSource.AVRO_SERDE_NAME);
  }

  private static String getKafkaTopicName(
      final PreparedStatement<AbstractStreamCreateStatement> statement
  ) {
    return getRequiredProperty(statement, DdlConfig.KAFKA_TOPIC_NAME_PROPERTY);
  }

  private static Optional<Integer> getSchemaId(
      final PreparedStatement<AbstractStreamCreateStatement> statement
  ) {
    try {
      return getOptionalProperty(statement, KsqlConstants.AVRO_SCHEMA_ID)
          .map(Integer::parseInt);
    } catch (final NumberFormatException e) {
      throw new KsqlStatementException(
          KsqlConstants.AVRO_SCHEMA_ID + " should be numeric",
          statement.getStatementText(),
          e);
    }
  }

  private static String getRequiredProperty(
      final PreparedStatement<AbstractStreamCreateStatement> statement,
      final String property
  ) {
    return getOptionalProperty(statement, property)
        .orElseThrow(() -> new KsqlStatementException(
            property + " should be set in WITH clause of CREATE STREAM/TABLE statement.",
            statement.getStatementText()));
  }

  private static Optional<String> getOptionalProperty(
      final PreparedStatement<AbstractStreamCreateStatement> statement,
      final String property
  ) {
    final Expression valueFormat = statement
        .getStatement()
        .getProperties()
        .get(property);

    if (valueFormat == null) {
      return Optional.empty();
    }

    if (!(valueFormat instanceof Literal)) {
      throw new KsqlStatementException(
          property + " should be set to a literal value in WITH clause.",
          statement.getStatementText());
    }

    return Optional.of(String.valueOf(((Literal) valueFormat).getValue()));
  }

  private static AbstractStreamCreateStatement addSchemaFields(
      final PreparedStatement<AbstractStreamCreateStatement> preparedStatement,
      final SchemaAndId schema
  ) {
    final List<TableElement> elements = buildElements(schema.schema, preparedStatement);

    final AbstractStreamCreateStatement statement = preparedStatement.getStatement();
    final Map<String, Expression> properties = new HashMap<>(statement.getProperties());
    properties
        .putIfAbsent(KsqlConstants.AVRO_SCHEMA_ID, new StringLiteral(String.valueOf(schema.id)));

    return statement.copyWith(elements, properties);
  }

  private static List<TableElement> buildElements(
      final Schema schema,
      final PreparedStatement<AbstractStreamCreateStatement> preparedStatement
  ) {
    try {
      return TableElement.fromSchema(schema);
    } catch (final Exception e) {
      throw new KsqlStatementException(
          "Failed to convert schema to KSQL model: " + e.getMessage(),
          preparedStatement.getStatementText(),
          e);
    }
  }

  private static PreparedStatement buildPreparedStatement(
      final AbstractStreamCreateStatement stmt
  ) {
    return PreparedStatement.of(SqlFormatter.formatSql(stmt), stmt);
  }
}
