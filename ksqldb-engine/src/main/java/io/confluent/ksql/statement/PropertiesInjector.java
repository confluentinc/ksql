/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.statement;

import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.KsqlConfig;
import java.util.HashMap;
import java.util.Map;

public class PropertiesInjector implements Injector {
  private <T extends Statement> boolean isSourceTable(final ConfiguredStatement<T> statement) {
    return statement.getStatement() instanceof CreateTable
        && ((CreateTable) statement.getStatement()).isSource();
  }

  @Override
  public <T extends Statement> ConfiguredStatement<T> inject(
      final ConfiguredStatement<T> statement
  ) {
    if (!isSourceTable(statement)) {
      return statement;
    }

    return ConfiguredStatement.of(
      KsqlParser.PreparedStatement.of(statement.getStatementText(), statement.getStatement()),
      forSourceTable(statement.getSessionConfig())
    );
  }

  public SessionConfig forSourceTable(final SessionConfig sessionConfig) {
    final KsqlConfig systemConfig = sessionConfig.getConfig(false);
    final Map<String, Object> overrides = new HashMap<>(sessionConfig.getOverrides());

    // Source tables must be materialized from the earliest offset. This config makes sure to
    // override system or a user overridden auto.offset.reset config.
    overrides.put("auto.offset.reset", "earliest");

    return SessionConfig.of(systemConfig, overrides);
  }
}
