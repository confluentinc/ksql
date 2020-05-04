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

import io.confluent.ksql.api.client.QueryResult;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.reactive.BufferedPublisher;
import io.vertx.core.Context;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.reactivestreams.Subscriber;

class QueryResultImpl extends BufferedPublisher<Row> implements QueryResult {

  private final String queryId;
  private final List<String> columnNames;
  private final List<String> columnTypes;
  private final PollableSubscriber pollableSubscriber;
  private volatile boolean polling;
  private boolean subscribing;

  QueryResultImpl(final Context context, final String queryId, final List<String> columnNames,
      final List<String> columnTypes) {
    super(context);
    this.queryId = queryId;
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
    this.pollableSubscriber = new PollableSubscriber(ctx, this::sendError);
  }

  @Override
  public List<String> columnNames() {
    return columnNames;
  }

  @Override
  public List<String> columnTypes() {
    return columnTypes;
  }

  @Override
  public String queryID() {
    return queryId;
  }

  @Override
  public void subscribe(final Subscriber<? super Row> subscriber) {
    if (polling) {
      throw new IllegalStateException("Cannot set subscriber if polling");
    }
    synchronized (this) {
      subscribing = true;
      super.subscribe(subscriber);
    }
  }

  @Override
  public Row poll() {
    return poll(0, TimeUnit.MILLISECONDS);
  }

  @Override
  public synchronized Row poll(final long timeout, final TimeUnit timeUnit) {
    if (subscribing) {
      throw new IllegalStateException("Cannot poll if subscriber has been set");
    }
    if (!polling) {
      subscribe(pollableSubscriber);
      subscribing = false;
      polling = true;
    }
    return pollableSubscriber.poll(timeout, timeUnit);
  }

  @Override
  public boolean isComplete() {
    return false;
  }

  public void handleError(final Exception e) {
    sendError(e);
  }

  @Override
  public void close() {
    pollableSubscriber.close();
  }

}