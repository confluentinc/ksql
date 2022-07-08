/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.rest.integration;

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.reactive.BaseSubscriber;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.vertx.core.Context;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.reactivestreams.Subscription;

/**
 * A useful subscriber for testing. It calls back on the given futures when the header is received
 * as well as the complete set of rows. Also, the method {@code getRows} can be called to poll the
 * currently received rows in a test.
 */
public class QueryStreamSubscriber extends BaseSubscriber<StreamedRow> {

  private final CompletableFuture<List<StreamedRow>> future;
  private final CompletableFuture<StreamedRow> header;
  private boolean closed;
  private List<StreamedRow> rows = new ArrayList<>();

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public QueryStreamSubscriber(
      final Context context,
      final CompletableFuture<List<StreamedRow>> future,
      final CompletableFuture<StreamedRow> header
  ) {
    super(context);
    this.future = Objects.requireNonNull(future);
    this.header = Objects.requireNonNull(header);
  }

  @Override
  protected void afterSubscribe(final Subscription subscription) {
    makeRequest(1);
  }

  @Override
  protected synchronized void handleValue(final StreamedRow row) {
    if (closed) {
      return;
    }
    rows.add(row);
    if (row.isTerminal()) {
      future.complete(rows);
      return;
    }
    if (row.getHeader().isPresent()) {
      header.complete(row);
    }
    makeRequest(1);
  }

  @Override
  protected void handleComplete() {
    future.complete(rows);
  }

  @Override
  protected void handleError(final Throwable t) {
    header.completeExceptionally(t);
    future.completeExceptionally(t);
  }

  public void close() {
    closed = true;
    context.runOnContext(v -> cancel());
  }

  public synchronized List<StreamedRow> getRows() {
    return ImmutableList.copyOf(rows);
  }

  public synchronized List<StreamedRow> getUniqueRows() {
    return rows.stream()
        .distinct()
        .collect(ImmutableList.toImmutableList());
  }
}
