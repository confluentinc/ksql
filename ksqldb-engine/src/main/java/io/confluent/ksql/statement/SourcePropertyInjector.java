/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.statement;

import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.ErrorMessageUtil;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;

/**
 * An injector which injects information into {@code CreateSourceProperties}
 * or {@code CreateSourceAsProperties}.
 *
 * <p>The only information injected currently is that new queries using the
 * protobuf format should use the new default converter behavior to unwrap
 * wrapped primitives. This injector causes new queries to use the new behavior
 * while old (existing) queries will continue to use the old behavior.
 *
 * <p>If a statement that is not {@code CreateAsSelect} or {@code CreateSource}
 * is passed in, this results in a no-op that returns the incoming statement.</p>
 */
public class SourcePropertyInjector implements Injector {

  @SuppressWarnings("unchecked")
  public <T extends Statement> ConfiguredStatement<T> inject(
      final ConfiguredStatement<T> statement
  ) {
    if (!(statement.getStatement() instanceof CreateSource)
        && !(statement.getStatement() instanceof CreateAsSelect)) {
      return statement;
    }

    try {
      if (statement.getStatement() instanceof CreateAsSelect) {
        return (ConfiguredStatement<T>) injectForCreateAsSelect(
            (ConfiguredStatement<? extends CreateAsSelect>) statement);
      } else {
        return (ConfiguredStatement<T>) injectForCreateSource(
            (ConfiguredStatement<? extends CreateSource>) statement);
      }
    } catch (final KsqlStatementException e) {
      throw e;
    } catch (final KsqlException e) {
      throw new KsqlStatementException(
          ErrorMessageUtil.buildErrorMessage(e),
          statement.getMaskedStatementText(),
          e.getCause());
    }
  }

  private ConfiguredStatement<? extends CreateSource> injectForCreateSource(
      final ConfiguredStatement<? extends CreateSource> original
  ) {
    final CreateSource statement = original.getStatement();
    final CreateSourceProperties properties = statement.getProperties();

    final CreateSourceProperties injectedProps = properties.withUnwrapProtobufPrimitives(true);

    return buildConfiguredStatement(original, injectedProps);
  }

  private ConfiguredStatement<? extends CreateAsSelect> injectForCreateAsSelect(
      final ConfiguredStatement<? extends CreateAsSelect> original
  ) {
    final CreateAsSelect createAsSelect = original.getStatement();
    final CreateSourceAsProperties properties = createAsSelect.getProperties();

    final CreateSourceAsProperties injectedProps = properties.withUnwrapProtobufPrimitives(true);

    return buildConfiguredStatement(original, injectedProps);
  }

  private static ConfiguredStatement<CreateSource> buildConfiguredStatement(
      final ConfiguredStatement<? extends CreateSource> original,
      final CreateSourceProperties injectedProps
  ) {
    final CreateSource statement = original.getStatement();

    final CreateSource withProps = statement.copyWith(
        original.getStatement().getElements(),
        injectedProps
    );

    final PreparedStatement<CreateSource> prepared = buildPreparedStatement(withProps);
    return ConfiguredStatement.of(prepared, original.getSessionConfig());
  }

  private static ConfiguredStatement<CreateAsSelect> buildConfiguredStatement(
      final ConfiguredStatement<? extends CreateAsSelect> original,
      final CreateSourceAsProperties injectedProps
  ) {
    final CreateAsSelect statement = original.getStatement();

    final CreateAsSelect withProps = statement.copyWith(injectedProps);

    final PreparedStatement<CreateAsSelect> prepared = buildPreparedStatement(withProps);
    return ConfiguredStatement.of(prepared, original.getSessionConfig());
  }

  private static <T extends Statement> PreparedStatement<T> buildPreparedStatement(
      final T stmt
  ) {
    return PreparedStatement.of(SqlFormatter.formatSql(stmt), stmt);
  }
}
