/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.parser.tree.Statement;

public final class StatementWithSchema {

  private StatementWithSchema() {
  }

  @SuppressWarnings("unchecked")
  public static <T extends Statement> PreparedStatement<T> forStatement(
      final PreparedStatement<T> statement,
      final SchemaRegistryClient schemaRegistryClient
  ) {
    if (!(statement.getStatement() instanceof AbstractStreamCreateStatement)) {
      return statement;
    }

    final AbstractStreamCreateStatement statementWithSchema = AvroUtil.checkAndSetAvroSchema(
        (AbstractStreamCreateStatement) statement.getStatement(), schemaRegistryClient);

    if (statementWithSchema == statement.getStatement()) {
      return statement;
    }

    return (PreparedStatement) PreparedStatement.of(
        SqlFormatter.formatSql(statementWithSchema),
        statementWithSchema);
  }
}
