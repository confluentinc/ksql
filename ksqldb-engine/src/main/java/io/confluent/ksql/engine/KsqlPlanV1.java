/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.engine;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.execution.ddl.commands.DdlCommand;
import java.util.Objects;
import java.util.Optional;

final class KsqlPlanV1 implements KsqlPlan {
  private final String statementText;
  private final Optional<DdlCommand> ddlCommand;
  private final Optional<QueryPlan> queryPlan;

  KsqlPlanV1(
      @JsonProperty(value = "statementText", required = true) final String statementText,
      @JsonProperty(value = "ddlCommand", required = true) final Optional<DdlCommand> ddlCommand,
      @JsonProperty(value = "queryPlan", required = true) final Optional<QueryPlan> queryPlan
  ) {
    this.statementText = Objects.requireNonNull(statementText, "statementText");
    this.ddlCommand = Objects.requireNonNull(ddlCommand, "ddlCommand");
    this.queryPlan = Objects.requireNonNull(queryPlan, "queryPlan");
  }

  public Optional<DdlCommand> getDdlCommand() {
    return ddlCommand;
  }

  public Optional<QueryPlan> getQueryPlan() {
    return queryPlan;
  }

  public String getStatementText() {
    return statementText;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KsqlPlanV1 that = (KsqlPlanV1) o;
    return Objects.equals(statementText, that.statementText)
        && Objects.equals(ddlCommand, that.ddlCommand)
        && Objects.equals(queryPlan, that.queryPlan);
  }

  @Override
  public int hashCode() {

    return Objects.hash(statementText, ddlCommand, queryPlan);
  }
}
