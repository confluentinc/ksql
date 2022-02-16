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

import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.api.client.ColumnType;
import io.confluent.ksql.api.client.Row;
import io.confluent.ksql.api.client.StreamedQueryResult;
import io.confluent.ksql.reactive.BufferedPublisher;
import io.confluent.ksql.util.KsqlRequestConfig;
import io.vertx.core.Context;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.time.Duration;
import java.util.List;
import org.reactivestreams.Subscriber;

public class StreamedQueryResultImpl extends BufferedPublisher<Row> implements StreamedQueryResult {

  private static final Logger log = LoggerFactory.getLogger(StreamedQueryResultImpl.class);

  private final String queryId;
  private final ImmutableList<String> columnNames;
  private final ImmutableList<ColumnType> columnTypes;
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
    this.columnNames = ImmutableList.copyOf(columnNames);
    this.columnTypes = ImmutableList.copyOf(columnTypes);
    this.pollableSubscriber = new PollableSubscriber(ctx, this::handleErrorWhilePolling);
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "columnNames is ImmutableList")
  public List<String> columnNames() {
    return columnNames;
  }

  @Override
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "columnTypes is ImmutableList")
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
    return poll(Duration.ZERO);
  }

  @Override
  public Row poll(final Duration timeout) {
    return poll(timeout, null);
  }

  private synchronized Row poll(final Duration timeout, final Runnable callback) {
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
    return pollableSubscriber.poll(timeout);
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

  private void handleErrorWhilePolling(final Throwable t) {
    log.error("Unexpected error while polling: " + t);
  }

  public void hasContinuationToken() {

    KsqlRequestConfig
  }

  public static Row pollWithCallback(
      final StreamedQueryResult queryResult,
      final Runnable callback
  ) {
    if (!(queryResult instanceof StreamedQueryResultImpl)) {
      throw new IllegalArgumentException("Can only poll with callback on StreamedQueryResultImpl");
    }
    final StreamedQueryResultImpl streamedQueryResult = (StreamedQueryResultImpl) queryResult;
    return streamedQueryResult.poll(Duration.ZERO, callback);
  }
}