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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import io.confluent.ksql.engine.KsqlPlan;
import io.confluent.ksql.planner.plan.ConfiguredKsqlPlan;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@JsonSubTypes({})
public class Command {
  private final String statement;
  private final Map<String, Object> overwriteProperties;
  private final Map<String, String> originalProperties;
  private final Optional<KsqlPlan> plan;

  @JsonCreator
  public Command(
      @JsonProperty(value = "statement", required = true) final String statement,
      @JsonProperty(value = "streamsProperties", required = true)
      final Map<String, Object> overwriteProperties,
      @JsonProperty(value = "originalProperties", required = true)
      final Map<String, String> originalProperties,
      @JsonProperty("plan")
      final Optional<KsqlPlan> plan
  ) {
    this.statement = statement;
    this.overwriteProperties = Collections.unmodifiableMap(overwriteProperties);
    this.originalProperties =
        originalProperties == null ? Collections.emptyMap() : originalProperties;
    this.plan = Objects.requireNonNull(plan, "plan");
  }

  public String getStatement() {
    return statement;
  }

  @JsonProperty("streamsProperties")
  public Map<String, Object> getOverwriteProperties() {
    return Collections.unmodifiableMap(overwriteProperties);
  }

  public Map<String, String> getOriginalProperties() {
    return originalProperties;
  }

  public Optional<KsqlPlan> getPlan() {
    return plan;
  }

  public static Command of(final ConfiguredKsqlPlan configuredPlan) {
    return new Command(
        configuredPlan.getPlan().getStatementText(),
        configuredPlan.getOverrides(),
        configuredPlan.getConfig().getAllConfigPropsWithSecretsObfuscated(),
        Optional.of(configuredPlan.getPlan())
    );
  }

  public static Command of(final ConfiguredStatement<?> configuredStatement) {
    return new Command(
        configuredStatement.getStatementText(),
        configuredStatement.getOverrides(),
        configuredStatement.getConfig().getAllConfigPropsWithSecretsObfuscated(),
        Optional.empty()
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
        + '}';
  }
}
