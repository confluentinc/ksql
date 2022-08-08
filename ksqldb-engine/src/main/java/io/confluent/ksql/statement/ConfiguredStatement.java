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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.testing.EffectivelyImmutable;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Objects;

/**
 * A prepared statement paired with the configurations needed to fully
 * execute it.
 */
@Immutable
public final class ConfiguredStatement<T extends Statement> {

  private final PreparedStatement<T> statement;
  @EffectivelyImmutable
  private final ImmutableMap<String, Object> configOverrides;
  @EffectivelyImmutable
  private final ImmutableMap<String, Object> requestProperties;
  private final KsqlConfig config;

  public static <S extends Statement> ConfiguredStatement<S> of(
      final PreparedStatement<S> statement,
      final Map<String, ?> overrides,
      final KsqlConfig config
  ) {
    return new ConfiguredStatement<>(statement, overrides, config);
  }

  public static <S extends Statement> ConfiguredStatement<S> of(
      final PreparedStatement<S> statement,
      final Map<String, ?> overrides,
      final Map<String, ?> requestProperties,
      final KsqlConfig config
  ) {
    return new ConfiguredStatement<>(statement, overrides, requestProperties, config);
  }

  private ConfiguredStatement(
      final PreparedStatement<T> statement,
      final Map<String, ?> configOverrides,
      final KsqlConfig config
  ) {
    this.statement = requireNonNull(statement, "statement");
    this.configOverrides = ImmutableMap.copyOf(requireNonNull(configOverrides, "overrides"));
    this.config = requireNonNull(config, "config");
    this.requestProperties = ImmutableMap.of();
  }

  private ConfiguredStatement(
      final PreparedStatement<T> statement,
      final Map<String, ?> configOverrides,
      final Map<String, ?> requestProperties,
      final KsqlConfig config
  ) {
    this.statement = requireNonNull(statement, "statement");
    this.configOverrides = ImmutableMap.copyOf(requireNonNull(configOverrides, "overrides"));
    this.requestProperties = ImmutableMap.copyOf(requireNonNull(
        requestProperties, "serverProperties"));
    this.config = requireNonNull(config, "config");
  }

  @SuppressWarnings("unchecked")
  public <S extends Statement> ConfiguredStatement<S> cast() {
    return (ConfiguredStatement<S>) this;
  }

  public T getStatement() {
    return statement.getStatement();
  }

  /**
   * Use masked statement for logging and other output places it could be read by human. It
   * masked sensitive information such as passwords, keys etc. For normal processing which
   * needs unmasked statement text, please use {@code getUnMaskedStatementText}
   * @return Masked statement text
   */
  public String getMaskedStatementText() {
    return statement.getMaskedStatementText();
  }

  /**
   * This method returns unmasked statement text which can be used for processing. For logging
   * and other output purposed for debugging etc, please use {@code getStatementText}
   * @return Masked statement text
   */
  public String getUnMaskedStatementText() {
    return statement.getUnMaskedStatementText();
  }

  public Map<String, Object> getConfigOverrides() {
    return configOverrides;
  }

  public Map<String, Object> getRequestProperties() {
    return requestProperties;
  }

  public KsqlConfig getConfig() {
    return config;
  }

  public ConfiguredStatement<T> withConfig(final KsqlConfig config) {
    return new ConfiguredStatement<>(this.statement, this.configOverrides, config);
  }

  public ConfiguredStatement<T> withConfigOverrides(final Map<String, Object> properties) {
    return new ConfiguredStatement<>(this.statement, properties, this.config);
  }

  public ConfiguredStatement<T> withRequestProperties(final Map<String, Object> properties) {
    return new ConfiguredStatement<>(this.statement, this.configOverrides, properties, this.config);
  }

  public ConfiguredStatement<T> withStatement(
      final String statementText,
      final T statement) {
    return new ConfiguredStatement<>(
        PreparedStatement.of(statementText, statement), this.configOverrides, this.config);
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
        && Objects.equals(configOverrides, that.configOverrides)
        && Objects.equals(config, that.config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statement, configOverrides, config);
  }

  @Override
  public String toString() {
    return "ConfiguredStatement{"
        + "statement=" + statement
        + ", configOverrides=" + configOverrides
        + ", requestProperties=" + requestProperties
        + ", config=" + config
        + '}';
  }
}
