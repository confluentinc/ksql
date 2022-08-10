/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.format;

import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import java.util.Optional;

/**
 * An injector which injects the key and value formats into the supplied {@code statement}.
 *
 * <p>The key format is only injected if:
 * <ul>
 * <li>The statement is a CT/CS.</li>
 * <li>The statement does not specify the FORMAT property in its WITH clause.</li>
 * <li>The statement does not specify the KEY_FORMAT property in its WITH clause.</li>
 * </ul>
 *
 * <p>Similarly, the value format is only injected if the above conditions are met,
 * where the KEY_FORMAT property is replaced with the VALUE_FORMAT property accordingly.
 *
 * <p>If any of the above are not true then the {@code statement} is returned unchanged.
 */
public class DefaultFormatInjector implements Injector {

  public DefaultFormatInjector() {
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends Statement> ConfiguredStatement<T> inject(
      final ConfiguredStatement<T> statement
  ) {
    if (statement.getStatement() instanceof CreateSource) {
      return handleCreateSource((ConfiguredStatement<CreateSource>) statement);
    }

    return statement;
  }

  @SuppressWarnings("unchecked")
  private <T extends Statement> ConfiguredStatement<T> handleCreateSource(
      final ConfiguredStatement<CreateSource> statement
  ) {
    try {
      final ConfiguredStatement<CreateSource> injected =
          injectForCreateStatement(statement).orElse(statement);

      // Safe to cast as we know `T` is `CreateSource`
      return (ConfiguredStatement<T>) injected;
    } catch (final KsqlStatementException e) {
      throw e;
    } catch (final KsqlException e) {
      throw new KsqlStatementException(
          e.getMessage(),
          statement.getMaskedStatementText(),
          e.getCause());
    }
  }

  private Optional<ConfiguredStatement<CreateSource>> injectForCreateStatement(
      final ConfiguredStatement<CreateSource> original
  ) {

    final CreateSource statement = original.getStatement();
    final CreateSourceProperties properties = statement.getProperties();

    final Optional<FormatInfo> keyFormat = properties.getKeyFormat(statement.getName());
    final Optional<FormatInfo> valueFormat = properties.getValueFormat();

    if (keyFormat.isPresent() && valueFormat.isPresent()) {
      return Optional.empty();
    }

    final KsqlConfig config = getConfig(original);

    final CreateSourceProperties injectedProps = properties.withFormats(
        keyFormat.map(FormatInfo::getFormat)
            .orElseGet(() -> getDefaultKeyFormat(config)),
        valueFormat.map(FormatInfo::getFormat)
            .orElseGet(() -> getDefaultValueFormat(config))
    );
    final CreateSource withFormats = statement.copyWith(
        original.getStatement().getElements(),
        injectedProps
    );

    final PreparedStatement<CreateSource> prepared = buildPreparedStatement(withFormats);
    final ConfiguredStatement<CreateSource> configured = ConfiguredStatement
        .of(prepared, original.getSessionConfig());

    return Optional.of(configured);
  }

  private static KsqlConfig getConfig(final ConfiguredStatement<?> statement) {
    return statement.getSessionConfig().getConfig(true);
  }

  private static String getDefaultKeyFormat(final KsqlConfig config) {
    final String format = config.getString(KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG);
    if (format == null) {
      throw new KsqlException("Statement is missing the '"
          + CommonCreateConfigs.KEY_FORMAT_PROPERTY + "' property from the WITH clause. "
          + "Either provide one or set a default via the '"
          + KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG + "' config.");
    }

    return format;
  }

  private static String getDefaultValueFormat(final KsqlConfig config) {
    final String format = config.getString(KsqlConfig.KSQL_DEFAULT_VALUE_FORMAT_CONFIG);
    if (format == null) {
      throw new KsqlException("Statement is missing the '"
          + CommonCreateConfigs.VALUE_FORMAT_PROPERTY + "' property from the WITH clause. "
          + "Either provide one or set a default via the '"
          + KsqlConfig.KSQL_DEFAULT_VALUE_FORMAT_CONFIG + "' config.");
    }

    return format;
  }

  private static PreparedStatement<CreateSource> buildPreparedStatement(
      final CreateSource stmt
  ) {
    return PreparedStatement.of(SqlFormatter.formatSql(stmt), stmt);
  }

}
