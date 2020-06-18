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
import io.confluent.ksql.rest.entity.TableRowsEntity;

public class PullQueryResult {

  private final TableRowsEntity tableRowsEntity;
  private final KsqlNode replyingNode;

  public PullQueryResult(
      final TableRowsEntity tableRowsEntity,
      final KsqlNode replyingNode) {

    this.tableRowsEntity = tableRowsEntity;
    this.replyingNode = replyingNode;
  }

  public TableRowsEntity getTableRowsEntity() {
    return tableRowsEntity;
  }

  public KsqlNode getReplyingNode() {
    return replyingNode;
  }
}
