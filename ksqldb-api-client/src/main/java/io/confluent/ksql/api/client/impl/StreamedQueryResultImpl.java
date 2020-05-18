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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.api.client.ColumnType;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.StreamedQueryResult;
import io.confluent.ksql.reactive.BufferedPublisher;
import io.vertx.core.Context;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.reactivestreams.Subscriber;

public class StreamedQueryResultImpl extends BufferedPublisher<Row> implements StreamedQueryResult {

  private static final Logger log = LoggerFactory.getLogger(StreamedQueryResultImpl.class);

  private final String queryId;
  private final List<String> columnNames;
  private final List<ColumnType> columnTypes;
  private final PollableSubscriber pollableSubscriber;
  private volatile boolean polling;
  private boolean subscribing;

  StreamedQueryResultImpl(
      final Context context,
      final String queryId,
      final List<String> columnNames,
      final List<ColumnType> columnTypes
  ) {
    super(context);
    this.queryId = queryId;
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
    this.pollableSubscriber = new PollableSubscriber(ctx, this::handleErrorWhilePolling);
  }

  @Override
  public List<String> columnNames() {
    return columnNames;
  }

  @Override
  public List<ColumnType> columnTypes() {
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
  public Row poll(final long timeout, final TimeUnit timeUnit) {
    return poll(timeout, timeUnit, null);
  }

  private synchronized Row poll(
      final long timeout,
      final TimeUnit timeUnit,
      final Runnable callback
  ) {
    if (subscribing) {
      throw new IllegalStateException("Cannot poll if subscriber has been set");
    }
    if (isFailed()) {
      throw new IllegalStateException(
          "Cannot poll on StreamedQueryResult that has failed. Check logs for failure reason.");
    }
    if (callback != null) {
      callback.run();
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
    return super.isComplete();
  }

  @Override
  public boolean isFailed() {
    return super.isFailed();
  }

  public void handleError(final Exception e) {
    sendError(e);
  }

  @Override
  public void close() {
    pollableSubscriber.close();
  }

  private void handleErrorWhilePolling(final Throwable t) {
    log.error("Unexpected error while polling: " + t);
  }

  @VisibleForTesting
  public static Row pollWithCallback(
      final StreamedQueryResult queryResult,
      final Runnable callback
  ) {
    if (!(queryResult instanceof StreamedQueryResultImpl)) {
      throw new IllegalArgumentException("Can only poll with callback on StreamedQueryResultImpl");
    }
    final StreamedQueryResultImpl streamedQueryResult = (StreamedQueryResultImpl) queryResult;
    return streamedQueryResult.poll(0, TimeUnit.MILLISECONDS, callback);
  }
}