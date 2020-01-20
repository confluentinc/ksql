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

package io.confluent.ksql.api;

import io.confluent.ksql.api.server.ReactiveSubscriber;
import io.confluent.ksql.api.spi.InsertsSubscriber;
import io.vertx.core.Context;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;
import org.reactivestreams.Subscription;

public class TestInsertsSubscriber extends ReactiveSubscriber<JsonObject> implements
    InsertsSubscriber {

  private final TestAcksPublisher acksPublisher;
  private final List<JsonObject> rowsInserted = new ArrayList<>();
  private boolean completed;

  public TestInsertsSubscriber(final Context context, final TestAcksPublisher acksPublisher) {
    super(context);
    this.acksPublisher = acksPublisher;
  }

  @Override
  protected void afterSubscribe(final Subscription subscription) {
    makeRequest(1);
  }

  @Override
  public synchronized void handleValue(final JsonObject row) {
    makeRequest(1);
    rowsInserted.add(row);
    if (acksPublisher != null) {
      // Now forward to acks publisher
      acksPublisher.accept(row);
    }
  }

  @Override
  public synchronized void handleComplete() {
    completed = true;
  }

  @Override
  public void handleError(final Throwable t) {
  }

  public synchronized List<JsonObject> getRowsInserted() {
    return rowsInserted;
  }

  public synchronized boolean isCompleted() {
    return completed;
  }

}
