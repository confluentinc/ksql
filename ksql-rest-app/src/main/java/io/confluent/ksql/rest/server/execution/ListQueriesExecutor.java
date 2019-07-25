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

package io.confluent.ksql.rest.server.execution;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.parser.tree.ListQueries;
import io.confluent.ksql.rest.entity.EntityQueryId;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.QueryDescription;
import io.confluent.ksql.rest.entity.QueryDescriptionList;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.Optional;
import java.util.stream.Collectors;

public final class ListQueriesExecutor {

  private ListQueriesExecutor() { }

  public static Optional<KsqlEntity> execute(
      final ConfiguredStatement<ListQueries> statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final ListQueries listQueries = statement.getStatement();
    if (listQueries.getShowExtended()) {
      return Optional.of(new QueryDescriptionList(
          statement.getStatementText(),
          executionContext.getPersistentQueries().stream()
              .map(QueryDescription::forQueryMetadata)
              .collect(Collectors.toList())));
    }

    return Optional.of(new io.confluent.ksql.rest.entity.Queries(
        statement.getStatementText(),
        executionContext.getPersistentQueries()
            .stream()
            .map(
                q -> new RunningQuery(
                    q.getStatementString(),
                    ImmutableSet.of(q.getSinkName()),
                    new EntityQueryId(q.getQueryId())))
            .collect(Collectors.toList())));
  }

}
