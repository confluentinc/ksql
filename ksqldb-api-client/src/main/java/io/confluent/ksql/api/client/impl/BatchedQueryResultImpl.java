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

package io.confluent.ksql.api.client.impl;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.api.client.BatchedQueryResult;
import java.util.concurrent.CompletableFuture;

public class BatchedQueryResultImpl extends BatchedQueryResult {

  private final CompletableFuture<String> queryId;

  BatchedQueryResultImpl() {
    this.queryId = new CompletableFuture<>();

    this.exceptionally(t -> {
      queryId.completeExceptionally(t);
      return null;
    });
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP")
  public CompletableFuture<String> queryID() {
    return queryId;
  }

}
