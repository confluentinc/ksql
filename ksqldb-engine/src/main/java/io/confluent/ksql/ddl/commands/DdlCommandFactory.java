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

package io.confluent.ksql.ddl.commands;

import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.execution.ddl.commands.DdlCommand;
import io.confluent.ksql.parser.tree.DdlStatement;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.serde.RefinementInfo;
import java.util.Optional;

public interface DdlCommandFactory {
  DdlCommand create(
      String sqlExpression,
      DdlStatement ddlStatement,
      SessionConfig config
  );

  DdlCommand create(
      KsqlStructuredDataOutputNode outputNode,
      Optional<RefinementInfo> emitStrategy
  );
}
