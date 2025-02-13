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
import io.confluent.ksql.execution.ddl.commands.CreateTableCommand;
import io.confluent.ksql.execution.ddl.commands.DdlCommand;
import io.confluent.ksql.util.KsqlConstants;
import java.util.Objects;
import java.util.Optional;

final class KsqlPlanV1 implements KsqlPlan {
  /**
   * This text should NEVER be parsed nor used when executing statements. Rather, it is only here to
   * display back to the user in the case of error messages; {@code EXPLAIN <query>},
   * {@code DESCRIBE <source>}, and other similar commands; and in case-insensitive string
   * comparison for {@code TERMINATE CLUSTER;} commands.
   *
   * <p>In light of the above, this field is NOT included in the {@code equals()} or
   * {@code hashCode()} methods of this class, and is also not validated to be unchanging by
   * query translation test historic plans.
   */
  private final String statementText;
  private final Optional<DdlCommand> ddlCommand;
  private final Optional<QueryPlan> queryPlan;

  KsqlPlanV1(
      @JsonProperty(value = "statementText", required = true) final String maskedStatement,
      @JsonProperty(value = "ddlCommand") final Optional<DdlCommand> ddlCommand,
      @JsonProperty(value = "queryPlan") final Optional<QueryPlan> queryPlan
  ) {
    this.statementText = Objects.requireNonNull(maskedStatement, "statementText");
    this.ddlCommand = Objects.requireNonNull(ddlCommand, "ddlCommand");
    this.queryPlan = Objects.requireNonNull(queryPlan, "queryPlan");

    if (!ddlCommand.isPresent() && !queryPlan.isPresent()) {
      throw new IllegalArgumentException("Plan requires at least a DDL command or query plan.");
    }
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
  public KsqlPlan withoutQuery() {
    return new KsqlPlanV1(statementText, ddlCommand, Optional.empty());
  }

  @Override
  public Optional<KsqlConstants.PersistentQueryType> getPersistentQueryType() {
    if (!queryPlan.isPresent()) {
      return Optional.empty();
    }

    // CREATE_AS and CREATE_SOURCE commands contain a DDL command and a Query plan.
    if (ddlCommand.isPresent()) {
      if (ddlCommand.get() instanceof CreateTableCommand
          && ((CreateTableCommand) ddlCommand.get()).getIsSource()) {
        return Optional.of(KsqlConstants.PersistentQueryType.CREATE_SOURCE);
      } else {
        return Optional.of(KsqlConstants.PersistentQueryType.CREATE_AS);
      }
    } else {
      // INSERT INTO persistent queries are the only queries types that exist without a
      // DDL command linked to the plan.
      return Optional.of(KsqlConstants.PersistentQueryType.INSERT);
    }
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
    return Objects.equals(ddlCommand, that.ddlCommand)
        && Objects.equals(queryPlan, that.queryPlan);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ddlCommand, queryPlan);
  }
}
