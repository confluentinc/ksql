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

package io.confluent.ksql.api;

import io.confluent.ksql.api.server.InsertResult;
import io.confluent.ksql.api.server.InsertsStreamSubscriber;
import io.confluent.ksql.reactive.BaseSubscriber;
import io.confluent.ksql.reactive.BufferedPublisher;
import io.vertx.core.Context;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;
import org.reactivestreams.Subscription;

public class TestInsertsSubscriber extends BaseSubscriber<JsonObject> implements
    InsertsStreamSubscriber {

  private final BufferedPublisher<InsertResult> acksPublisher;
  private final List<JsonObject> rowsInserted = new ArrayList<>();
  private final int acksBeforePublisherError;
  private boolean completed;
  private long seq;
  private boolean closed;

  public TestInsertsSubscriber(final Context context,
      final BufferedPublisher<InsertResult> acksPublisher,
      final int acksBeforePublisherError) {
    super(context);
    this.acksPublisher = acksPublisher;
    this.acksBeforePublisherError = acksBeforePublisherError;
  }

  @Override
  protected void afterSubscribe(final Subscription subscription) {
    makeRequest(1);
  }

  @Override
  public synchronized void handleValue(final JsonObject row) {
    makeRequest(1);
    rowsInserted.add(row);
    if (acksBeforePublisherError != -1 && seq == acksBeforePublisherError) {
      // We inject an exception after a certain number of rows
      acksPublisher
          .accept(InsertResult.failedInsert(seq, new RuntimeException("Failure in processing")));
    } else {
      acksPublisher.accept(InsertResult.succeededInsert(seq));
    }
    seq++;
  }

  @Override
  public synchronized void handleComplete() {
    completed = true;
  }

  @Override
  public void handleError(final Throwable t) {
  }

  @Override
  public synchronized void close() {
    closed = true;
  }

  public synchronized List<JsonObject> getRowsInserted() {
    return new ArrayList<>(rowsInserted);
  }

  public synchronized boolean isCompleted() {
    return completed;
  }

  public synchronized boolean isClosed() {
    return closed;
  }

}
