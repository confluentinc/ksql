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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.confluent.ksql.execution.ddl.commands.DdlCommand;
import io.confluent.ksql.util.KsqlConstants;
import java.util.Optional;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({
    @Type(value = KsqlPlanV1.class, name = "ksqlPlanV1")
})
public interface KsqlPlan {

  Optional<DdlCommand> getDdlCommand();

  Optional<QueryPlan> getQueryPlan();

  String getStatementText();

  KsqlPlan withoutQuery();

  @JsonIgnore
  Optional<KsqlConstants.PersistentQueryType> getPersistentQueryType();

  static KsqlPlan ddlPlanCurrent(final String statementText, final DdlCommand ddlCommand) {
    return new KsqlPlanV1(statementText, Optional.of(ddlCommand), Optional.empty());
  }

  static KsqlPlan queryPlanCurrent(
      final String maskedStatement,
      final Optional<DdlCommand> ddlCommand,
      final QueryPlan queryPlan
  ) {
    return new KsqlPlanV1(maskedStatement, ddlCommand, Optional.of(queryPlan));
  }
}
