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
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.statement.Injector;
import io.confluent.ksql.util.ErrorMessageUtil;
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
 *
 * <p>This injector also validates that the KEY_FORMAT property is only supplied if the
 * relevant feature flag is enabled.
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
      final ConfiguredStatement<CreateSource> createStatement =
          (ConfiguredStatement<CreateSource>) statement;

      if (createStatement.getStatement().getProperties().getKeyFormatName().isPresent()) {
        throwIfKeyFormatDisabled(createStatement);
      }

      try {
        return (ConfiguredStatement<T>) forCreateStatement(createStatement).orElse(createStatement);
      } catch (final KsqlStatementException e) {
        throw e;
      } catch (final KsqlException e) {
        throw new KsqlStatementException(
            ErrorMessageUtil.buildErrorMessage(e),
            statement.getStatementText(),
            e.getCause());
      }
    }

    if (statement.getStatement() instanceof CreateAsSelect) {
      final ConfiguredStatement<CreateAsSelect> createAsSelect =
          (ConfiguredStatement<CreateAsSelect>) statement;

      if (createAsSelect.getStatement().getProperties().getKeyFormatInfo().isPresent()) {
        throwIfKeyFormatDisabled(createAsSelect);
      }
    }

    return statement;
  }

  private Optional<ConfiguredStatement<CreateSource>> forCreateStatement(
      final ConfiguredStatement<CreateSource> original
  ) {

    final CreateSource statement = original.getStatement();
    final CreateSourceProperties properties = statement.getProperties();

    final Optional<String> keyFormat = properties.getKeyFormatName();
    final Optional<String> valueFormat = properties.getValueFormatName();

    final KsqlConfig config = getConfig(original);
    if (!config.getBoolean(KsqlConfig.KSQL_KEY_FORMAT_ENABLED)
        && !valueFormat.isPresent()) {
      throw new ParseFailedException("Failed to prepare statement: Missing required property "
          + "\"VALUE_FORMAT\" which has no default value.");
    }

    if (keyFormat.isPresent() && valueFormat.isPresent()) {
      return Optional.empty();
    }

    validateConfig(config, keyFormat, valueFormat);

    final CreateSourceProperties injectedProps = properties.withFormats(
        keyFormat.orElse(config.getString(KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG)),
        valueFormat.orElse(config.getString(KsqlConfig.KSQL_DEFAULT_VALUE_FORMAT_CONFIG))
    );
    final CreateSource withFormats = statement.copyWith(
        original.getStatement().getElements(),
        injectedProps
    );

    final PreparedStatement<CreateSource> prepared = buildPreparedStatement(withFormats);
    final ConfiguredStatement<CreateSource> configured = ConfiguredStatement
        .of(prepared, original.getConfigOverrides(), original.getConfig());

    return Optional.of(configured);
  }

  private static void validateConfig(
      final KsqlConfig config,
      final Optional<String> keyFormat,
      final Optional<String> valueFormat
  ) {
    if (!keyFormat.isPresent()
        && (config.getString(KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG) == null)) {
      throw new KsqlException("Statement is missing the '"
          + CommonCreateConfigs.KEY_FORMAT_PROPERTY + "' property from the WITH clause. "
          + "Either provide one or set a default via the '"
          + KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG + "' config.");
    }
    if (!valueFormat.isPresent()
        && (config.getString(KsqlConfig.KSQL_DEFAULT_VALUE_FORMAT_CONFIG) == null)) {
      throw new KsqlException("Statement is missing the '"
          + CommonCreateConfigs.VALUE_FORMAT_PROPERTY + "' property from the WITH clause. "
          + "Either provide one or set a default via the '"
          + KsqlConfig.KSQL_DEFAULT_VALUE_FORMAT_CONFIG + "' config.");
    }
  }

  private static <T extends Statement> KsqlConfig getConfig(
      final ConfiguredStatement<T> statement
  ) {
    return statement.getConfig().cloneWithPropertyOverwrite(statement.getConfigOverrides());
  }

  private static <T extends Statement> void throwIfKeyFormatDisabled(
      final ConfiguredStatement<T> statement
  ) {
    if (!getConfig(statement).getBoolean(KsqlConfig.KSQL_KEY_FORMAT_ENABLED)) {
      throw new KsqlStatementException(
          "The use of '" + CommonCreateConfigs.KEY_FORMAT_PROPERTY + "' and '"
              + CommonCreateConfigs.FORMAT_PROPERTY + "' is disabled, as this feature is "
              + "under development. Set '" + KsqlConfig.KSQL_KEY_FORMAT_ENABLED + "' to enable.",
          statement.getStatementText()
      );
    }
  }

  private static PreparedStatement<CreateSource> buildPreparedStatement(
      final CreateSource stmt
  ) {
    return PreparedStatement.of(SqlFormatter.formatSql(stmt), stmt);
  }
}
