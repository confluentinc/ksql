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

package io.confluent.ksql.api;

import io.confluent.ksql.api.spi.InsertsSubscriber;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;
import org.reactivestreams.Subscription;

public class TestInsertsSubscriber implements InsertsSubscriber {

  private final TestAcksPublisher acksPublisher;
  private final List<JsonObject> rowsInserted = new ArrayList<>();
  private boolean completed;
  private Subscription subscription;

  public TestInsertsSubscriber(final TestAcksPublisher acksPublisher) {
    this.acksPublisher = acksPublisher;
  }

  @Override
  public void onSubscribe(final Subscription subscription) {
    if (this.subscription != null) {
      throw new IllegalStateException("Already subscribed");
    }
    this.subscription = subscription;
    subscription.request(1);
  }

  @Override
  public synchronized void onNext(final JsonObject row) {
    System.out.println("Received row in inserts subscriber");
    rowsInserted.add(row);
    subscription.request(1);
    if (acksPublisher != null) {
      // Now forward to acks publisher
      acksPublisher.receiveRow(row);
    }
  }

  @Override
  public void onError(final Throwable throwable) {
  }

  @Override
  public synchronized void onComplete() {
    this.completed = true;
  }

  public List<JsonObject> getRowsInserted() {
    return rowsInserted;
  }

  public boolean isCompleted() {
    return completed;
  }
}
