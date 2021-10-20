/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.engine;

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;

public class SqlFormatInjector implements Injector {

  private final KsqlExecutionContext executionContext;

  public SqlFormatInjector(final KsqlExecutionContext executionContext) {
    this.executionContext = Objects.requireNonNull(executionContext, "executionContext");
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends Statement> ConfiguredStatement<T> inject(
      final ConfiguredStatement<T> statement
  ) {
    try {
      final Statement node = statement.getStatement();
      final String sql = SqlFormatter.formatSql(node);
      final String sqlWithSemiColon = sql.endsWith(";") ? sql : sql + ";";
      final PreparedStatement<?> prepare = executionContext
          .prepare(executionContext.parse(sqlWithSemiColon).get(0));
      final Statement finalStatement;

      // Put Elements in CreateAsSelect back
      if (node instanceof CreateAsSelect && ((CreateAsSelect) node).getElements().isPresent()) {
        final CreateAsSelect cas = ((CreateAsSelect) prepare.getStatement());
        finalStatement = cas.copyWith(((CreateAsSelect) node).getElements().get(),
            cas.getProperties());
      } else {
        finalStatement = prepare.getStatement();
      }

      return statement.withStatement(sql, (T) finalStatement);
    } catch (final Exception e) {
      throw new KsqlException("Unable to format statement! This is bad because "
          + "it means we cannot persist it onto the command topic: " + statement, e);
    }
  }
}