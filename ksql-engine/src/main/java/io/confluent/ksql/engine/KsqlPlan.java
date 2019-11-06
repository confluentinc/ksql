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

import io.confluent.ksql.execution.ddl.commands.DdlCommand;
import java.util.Objects;
import java.util.Optional;

public final class KsqlPlan {

  private final String statementText;
  private final Optional<DdlCommand> ddlCommand;
  private final Optional<QueryPlan> queryPlan;

  static KsqlPlan ddlPlan(
      final String statementText,
      final DdlCommand ddlCommand
  ) {
    return new KsqlPlan(statementText, Optional.of(ddlCommand), Optional.empty());
  }

  static KsqlPlan queryPlan(
      final String statementText,
      final Optional<DdlCommand> ddlCommand,
      final QueryPlan queryPlan
  ) {
    return new KsqlPlan(statementText, ddlCommand, Optional.of(queryPlan));
  }

  private KsqlPlan(
      final String statementText,
      final Optional<DdlCommand> ddlCommand,
      final Optional<QueryPlan> queryPlan
  ) {
    this.statementText = Objects.requireNonNull(statementText, "statementText");
    this.ddlCommand = Objects.requireNonNull(ddlCommand, "ddlCommand");
    this.queryPlan = Objects.requireNonNull(queryPlan, "queryPlan");
  }

  Optional<DdlCommand> getDdlCommand() {
    return ddlCommand;
  }

  Optional<QueryPlan> getQueryPlan() {
    return queryPlan;
  }

  public String getStatementText() {
    return statementText;
  }
}
