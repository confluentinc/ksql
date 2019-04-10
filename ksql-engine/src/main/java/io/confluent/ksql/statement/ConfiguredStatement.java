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

package io.confluent.ksql.statement;

import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Objects;

/**
 * A prepared statement paired with the configurations needed to fully
 * execute it.
 */
public final class ConfiguredStatement<T extends Statement> {

  private final PreparedStatement<T> statement;
  private final Map<String, Object> overrides;
  private final KsqlConfig config;

  public static <S extends Statement> ConfiguredStatement<S> of(
      final PreparedStatement<S> statement,
      final Map<String, Object> overrides,
      final KsqlConfig config
  ) {
    return new ConfiguredStatement<>(statement, overrides, config);
  }

  private ConfiguredStatement(
      final PreparedStatement<T> statement,
      final Map<String, Object> overrides,
      final KsqlConfig config
  ) {
    this.statement = Objects.requireNonNull(statement, "statement");
    this.overrides = Objects.requireNonNull(overrides, "overrides");
    this.config = Objects.requireNonNull(config, "config");
  }

  private ConfiguredStatement(
      final ConfiguredStatement<T> other,
      final KsqlConfig config
  ) {
    this(other.statement, other.overrides, config);
  }

  private ConfiguredStatement(
      final ConfiguredStatement<T> other,
      final Map<String, Object> properties
  ) {
    this(other.statement, properties, other.config);
  }

  public T getStatement() {
    return statement.getStatement();
  }

  public String getStatementText() {
    return statement.getStatementText();
  }

  public Map<String, Object> getOverrides() {
    return overrides;
  }

  public KsqlConfig getConfig() {
    return config;
  }

  public ConfiguredStatement<T> withConfig(final KsqlConfig config) {
    return new ConfiguredStatement<>(this, config);
  }

  public ConfiguredStatement<T> withProperties(final Map<String, Object> properties) {
    return new ConfiguredStatement<>(this, properties);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ConfiguredStatement<?> that = (ConfiguredStatement<?>) o;
    return Objects.equals(statement, that.statement)
        && Objects.equals(overrides, that.overrides)
        && Objects.equals(config, that.config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statement, overrides, config);
  }

  @Override
  public String toString() {
    return "ConfiguredStatement{"
        + "statement=" + statement
        + ", overrides=" + overrides
        + ", config=" + config
        + '}';
  }
}
