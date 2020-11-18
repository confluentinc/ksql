/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.physical.pull;

import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.List;
import java.util.Optional;

public class PullQueryResult {

  private final List<List<?>> tableRows;
  private final Optional<List<KsqlNode>> sourceNodes;
  private final LogicalSchema schema;
  private final QueryId queryId;

  public PullQueryResult(
      final List<List<?>> tableRowsEntity,
      final Optional<List<KsqlNode>> sourceNodes,
      final LogicalSchema schema,
      final QueryId queryId
  ) {

    this.tableRows = tableRowsEntity;
    this.sourceNodes = sourceNodes;
    this.schema = schema;
    this.queryId = queryId;
  }

  public LogicalSchema getSchema() {
    return schema;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public List<List<?>> getTableRows() {
    return tableRows;
  }

  public Optional<List<KsqlNode>> getSourceNodes() {
    return sourceNodes;
  }

}
