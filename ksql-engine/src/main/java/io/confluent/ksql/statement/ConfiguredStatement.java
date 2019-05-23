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
import java.util.Optional;

/**
 * A prepared statement paired with the configurations needed to fully
 * execute it.
 */
public final class ConfiguredStatement<T extends Statement> {

  private final PreparedStatement<T> statement;
  private final Map<String, Object> overrides;
  private final KsqlConfig config;
  private final Optional<Checksum> checksum;

  public static <S extends Statement> ConfiguredStatement<S> of(
      final PreparedStatement<S> statement,
      final Map<String, Object> overrides,
      final KsqlConfig config,
      final Checksum checksum
  ) {
    return of(statement, overrides, config, Optional.of(checksum));
  }

  public static <S extends Statement> ConfiguredStatement<S> of(
      final PreparedStatement<S> statement,
      final Map<String, Object> overrides,
      final KsqlConfig config,
      final Optional<Checksum> checksum
  ) {
    return new ConfiguredStatement<>(statement, overrides, config, checksum);
  }

  public static <S extends Statement> ConfiguredStatement<S> of(
      final PreparedStatement<S> statement,
      final Map<String, Object> overrides,
      final KsqlConfig config
  ) {
    return of(statement, overrides, config, Optional.empty());
  }

  private ConfiguredStatement(
      final PreparedStatement<T> statement,
      final Map<String, Object> overrides,
      final KsqlConfig config,
      final Optional<Checksum> checksum
  ) {
    this.statement = Objects.requireNonNull(statement, "statement");
    this.overrides = Objects.requireNonNull(overrides, "overrides");
    this.config = Objects.requireNonNull(config, "config");
    this.checksum = Objects.requireNonNull(checksum, "checksum");
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

  public Optional<Checksum> getChecksum() {
    return checksum;
  }

  public ConfiguredStatement<T> withConfig(final KsqlConfig config) {
    return new ConfiguredStatement<>(this.statement, this.overrides, config, this.checksum);
  }

  public ConfiguredStatement<T> withProperties(final Map<String, Object> properties) {
    return new ConfiguredStatement<>(this.statement, properties, this.config, this.checksum);
  }

  public ConfiguredStatement<T> withStatement(
      final String statementText,
      final T statement) {
    return new ConfiguredStatement<>(
        PreparedStatement.of(statementText, statement), this.overrides, this.config, this.checksum);
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
        && Objects.equals(config, that.config)
        && Objects.equals(checksum, that.checksum);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statement, overrides, config, checksum);
  }

  @Override
  public String toString() {
    return "ConfiguredStatement{"
        + "statement=" + statement
        + ", overrides=" + overrides
        + ", config=" + config
        + ", checksum=" + checksum
        + '}';
  }
}
