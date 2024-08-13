/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.server.computation;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.KsqlPlan;
import io.confluent.ksql.planner.plan.ConfiguredKsqlPlan;
import io.confluent.ksql.properties.PropertiesUtil;
import io.confluent.ksql.rest.server.resources.IncompatibleKsqlCommandVersionException;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@JsonSubTypes({})
public class Command {

  @VisibleForTesting
  public static final int VERSION = 18;

  private final String statement;
  private final Map<String, Object> overwriteProperties;
  private final Map<String, String> originalProperties;
  private final Optional<KsqlPlan> plan;
  private final Optional<Integer> version;

  @JsonCreator
  public Command(
      @JsonProperty(value = "statement", required = true) final String statement,
      @JsonProperty("streamsProperties") final Optional<Map<String, Object>> overwriteProperties,
      @JsonProperty("originalProperties") final Optional<Map<String, String>> originalProperties,
      @JsonProperty("plan") final Optional<KsqlPlan> plan,
      @JsonProperty("version") final Optional<Integer> version
  ) {
    this(
        statement,
        overwriteProperties.orElseGet(ImmutableMap::of),
        originalProperties.orElseGet(ImmutableMap::of),
        plan,
        version,
        VERSION
    );
  }

  @VisibleForTesting
  public Command(
      final String statement,
      final Map<String, Object> overwriteProperties,
      final Map<String, String> originalProperties,
      final Optional<KsqlPlan> plan
  ) {
    this(
        statement,
        overwriteProperties,
        originalProperties,
        plan,
        Optional.of(VERSION),
        VERSION
    );
  }

  @VisibleForTesting
  public Command(
      final String statement,
      final Map<String, Object> overwriteProperties,
      final Map<String, String> originalProperties,
      final Optional<KsqlPlan> plan,
      final Optional<Integer> version,
      final int expectedVersion
  ) {
    this.statement = requireNonNull(statement, "statement");
    // cannot use ImmutableMap, because we need to handle null-values
    this.overwriteProperties = Collections.unmodifiableMap(
        requireNonNull(overwriteProperties, "overwriteProperties"));
    // cannot use ImmutableMap, because we need to handle null-values
    this.originalProperties = Collections.unmodifiableMap(
        requireNonNull(originalProperties, "originalProperties"));
    this.plan = requireNonNull(plan, "plan");
    this.version = requireNonNull(version, "version");

    if (expectedVersion < version.orElse(0)) {
      throw new IncompatibleKsqlCommandVersionException(
          "Received a command from an incompatible command topic version. "
              + "Expected version less than or equal to " + expectedVersion
              + " but got " + version.orElse(0));
    }
  }

  public String getStatement() {
    return statement;
  }

  @JsonProperty("streamsProperties")
  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP",
      justification = "overwriteProperties is unmodifiableMap()"
  )
  public Map<String, Object> getOverwriteProperties() {
    return PropertiesUtil.coerceTypes(overwriteProperties, true);
  }

  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP",
      justification = "originalProperties is unmodifiableMap()"
  )
  public Map<String, String> getOriginalProperties() {
    return originalProperties;
  }

  public Optional<KsqlPlan> getPlan() {
    return plan;
  }

  public Optional<Integer> getVersion() {
    return version;
  }

  public static Command of(final ConfiguredKsqlPlan configuredPlan) {
    return new Command(
        configuredPlan.getPlan().getStatementText(),
        configuredPlan.getConfig().getOverrides(),
        configuredPlan.getConfig().getConfig(false).getAllConfigPropsWithSecretsObfuscated(),
        Optional.of(configuredPlan.getPlan()),
        Optional.of(VERSION),
        VERSION
    );
  }

  public static Command of(final ConfiguredStatement<?> configuredStatement) {
    final SessionConfig sessionConfig = configuredStatement.getSessionConfig();

    return new Command(
        configuredStatement.getUnMaskedStatementText(),
        sessionConfig.getOverrides(),
        sessionConfig.getConfig(false).getAllConfigPropsWithSecretsObfuscated(),
        Optional.empty(),
        Optional.of(VERSION),
        VERSION
    );
  }

  @Override
  public boolean equals(final Object o) {
    return
        o instanceof Command
        && Objects.equals(statement, ((Command)o).statement)
        && Objects.equals(overwriteProperties, ((Command)o).overwriteProperties)
        && Objects.equals(originalProperties, ((Command)o).originalProperties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statement, overwriteProperties, originalProperties);
  }

  @Override
  public String toString() {
    return "Command{"
        + "statement='" + statement + '\''
        + ", overwriteProperties=" + overwriteProperties
        + ", version=" + version
        + '}';
  }

}
