/*
 * Copyright 2020 Confluent Inc.
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

import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
import io.confluent.ksql.rest.entity.TableRows;
import java.util.Optional;

public class PullQueryResult {

  private final TableRows tableRows;
  private final Optional<KsqlNode> sourceNode;

  public PullQueryResult(
      final TableRows tableRowsEntity,
      final Optional<KsqlNode> sourceNode) {

    this.tableRows = tableRowsEntity;
    this.sourceNode = sourceNode;
  }

  public TableRows getTableRows() {
    return tableRows;
  }

  public Optional<KsqlNode> getSourceNode() {
    return sourceNode;
  }
}
