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

import io.confluent.ksql.api.spi.QueryPublisher;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import java.util.Iterator;
import java.util.List;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class TestQueryPublisher implements QueryPublisher {

  interface RowGenerator {

    JsonArray getColumnNames();

    JsonArray getColumnTypes();

    JsonArray getNext();

    int rowCount();
  }

  public static class ListRowGenerator implements RowGenerator {

    private final JsonArray columnNames;
    private final JsonArray columnTypes;
    private final int rowCount;
    private Iterator<JsonArray> iter;

    public ListRowGenerator(final JsonArray columnNames, final JsonArray columnTypes,
        final List<JsonArray> rows) {
      this.columnNames = columnNames;
      this.columnTypes = columnTypes;
      this.iter = rows.iterator();
      this.rowCount = rows.size();
    }

    @Override
    public JsonArray getColumnNames() {
      return columnNames;
    }

    @Override
    public JsonArray getColumnTypes() {
      return columnTypes;
    }

    @Override
    public JsonArray getNext() {
      if (iter.hasNext()) {
        return iter.next();
      } else {
        return null;
      }
    }

    @Override
    public int rowCount() {
      return rowCount;
    }
  }

  private final Vertx vertx;
  private final RowGenerator rowGenerator;
  private final int rowsBeforePublisherError;
  private final boolean push;
  private Subscriber<? super JsonArray> subscriber;
  private int rowsSent;

  public TestQueryPublisher(final Vertx vertx,
      final RowGenerator rowGenerator, final int rowsBeforePublisherError,
      final boolean push) {
    this.vertx = vertx;
    this.rowGenerator = rowGenerator;
    this.rowsBeforePublisherError = rowsBeforePublisherError;
    this.push = push;
  }


  @Override
  public JsonArray getColumnNames() {
    return rowGenerator.getColumnNames();
  }

  @Override
  public JsonArray getColumnTypes() {
    return rowGenerator.getColumnTypes();
  }

  @Override
  public int getRowCount() {
    return rowGenerator.rowCount();
  }

  @Override
  public synchronized void subscribe(final Subscriber<? super JsonArray> subscriber) {
    this.subscriber = subscriber;
    subscriber.onSubscribe(new TestSubscription());
  }

  synchronized boolean hasSubscriber() {
    return subscriber != null;
  }

  private synchronized void cancel() {
    if (subscriber == null) {
      return;
    }
    // TODO we should really deliver all events async
    Subscriber<? super JsonArray> theSub = subscriber;
    subscriber = null;
    theSub.onComplete();
  }

  private void sendAsync(long amount) {
    vertx.runOnContext(v -> send(amount));
  }

  private synchronized void send(long amount) {
    if (subscriber == null) {
      return;
    }
    for (int i = 0; i < amount; i++) {
      JsonArray row = rowGenerator.getNext();
      if (row == null) {
        if (!push) {
          cancel();
        }
        break;
      } else {
        if (rowsBeforePublisherError != -1 && rowsSent == rowsBeforePublisherError) {
          // Inject an error
          Subscriber<? super JsonArray> theSub = subscriber;
          subscriber = null;
          theSub.onError(new RuntimeException("Failure in processing"));
        } else {
          rowsSent++;
          subscriber.onNext(row);
        }
      }
    }
  }

  class TestSubscription implements Subscription {

    @Override
    public void request(final long l) {
      sendAsync(l);
    }

    @Override
    public void cancel() {
      TestQueryPublisher.this.cancel();
    }
  }

}
