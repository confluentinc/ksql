/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.util;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.parser.tree.Statement;

import java.util.Map;

public class StatementWithSchema {
  private final Statement statement;
  private final String statementText;

  private StatementWithSchema(final Statement statement, final String statementText) {
    this.statement = statement;
    this.statementText = statementText;
  }

  public Statement getStatement() {
    return statement;
  }

  public String getStatementText() {
    return statementText;
  }

  public static StatementWithSchema forStatement(
      final Statement statement,
      final String statementText,
      final Map<String, Object> properties,
      final SchemaRegistryClient schemaRegistryClient) {
    if (statement instanceof AbstractStreamCreateStatement) {
      final Statement statementWithSchema
          = AvroUtil.checkAndSetAvroSchema(
              (AbstractStreamCreateStatement) statement, properties, schemaRegistryClient);
      if (statementWithSchema != statement) {
        return new StatementWithSchema(
            statementWithSchema,
            SqlFormatter.formatSql(statementWithSchema)
        );
      }
    }
    return new StatementWithSchema(statement, statementText);
  }
}
