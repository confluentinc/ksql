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

package io.confluent.ksql.engine;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.KsqlConfig;
import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monitor and manages the restart of persistent failed queries.
 */
public class QueryMonitor implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(QueryMonitor.class);

  private static final Random random = new Random();
  private static final Ticker CURRENT_TIME_MILLIS_TICKER = new Ticker() {
    @Override
    public long read() {
      return System.currentTimeMillis();
    }
  };

  private static final long BASE_WAITING_TIME_MS = 10000;
  private static final int SHUTDOWN_TIMEOUT_MS = 5000;

  private final Ticker ticker;
  private final long retryBackoffMaxMs;
  private final KsqlEngine ksqlEngine;
  private final ExecutorService executor;
  private final Map<QueryId, RetryEvent> queriesRetries = new HashMap<>();

  private volatile boolean closed = false;

  public QueryMonitor(final KsqlConfig ksqlConfig, final KsqlEngine ksqlEngine) {
    this(
        ksqlConfig,
        ksqlEngine,
        Executors.newSingleThreadExecutor(r -> new Thread(r, "QueryMonitor")),
        CURRENT_TIME_MILLIS_TICKER
    );
  }

  @VisibleForTesting
  QueryMonitor(
      final KsqlConfig ksqlConfig,
      final KsqlEngine ksqlEngine,
      final ExecutorService executor,
      final Ticker ticker
  ) {
    this.retryBackoffMaxMs = ksqlConfig.getLong(KsqlConfig.KSQL_QUERY_RETRY_BACKOFF_MAX_MS);
    this.ksqlEngine = ksqlEngine;
    this.executor = executor;
    this.ticker = ticker;
  }

  public void start() {
    executor.execute(new Runner());
    executor.shutdown();
  }

  @Override
  public void close() {
    closed = true;

    try {
      executor.awaitTermination(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  boolean isClosed() {
    return closed;
  }

  void restartFailedQueries() {
    ksqlEngine.getPersistentQueries().stream()
        .forEach(query -> {
          final QueryId queryId = query.getQueryId();
          final KafkaStreams.State queryState = query.getState();

          // If the query was restarted previously, check if it needs another restart; or if the
          // query is now up and running, then remove it from the restart list.
          if (queriesRetries.containsKey(queryId)) {
            final RetryEvent retryEvent = queriesRetries.get(queryId);
            if (ticker.read() > retryEvent.nextRestartTimeMs()) {
              if (queryState == KafkaStreams.State.ERROR) {
                // Retry again if it's still in ERROR state
                retryEvent.restart();
              } else {
                // Query is not in ERROR state anymore.
                queriesRetries.remove(queryId);
              }
            }
          } else if (queryState == KafkaStreams.State.ERROR) {
            // Restart new query in ERROR state, and add it to the list of retries.
            final RetryEvent retryEvent = new RetryEvent(
                ksqlEngine, queryId, BASE_WAITING_TIME_MS, retryBackoffMaxMs, ticker);

            queriesRetries.put(queryId, retryEvent);
            retryEvent.restart();
          }
        });
  }

  private class Runner implements Runnable {
    @Override
    public void run() {
      LOG.info("KSQL query monitor started.");

      while (!closed) {
        try {
          restartFailedQueries();
        } catch (final Exception e) {
          LOG.warn("KSQL query monitor found an error attempting to restart failed queries.", e);
        }
      }
    }
  }

  static class RetryEvent {
    private final KsqlEngine ksqlEngine;
    private final QueryId queryId;
    private final Ticker ticker;

    private int numRetries = 0;
    private long waitingTimeMs;
    private long expiryTimeMs;
    private long retryBackoffMaxMs;
    private long baseWaitingTimeMs;

    RetryEvent(
        final KsqlEngine ksqlEngine,
        final QueryId queryId,
        final long baseWaitingTimeMs,
        final long retryBackoffMaxMs,
        final Ticker ticker
    ) {
      this.ksqlEngine = ksqlEngine;
      this.queryId = queryId;
      this.ticker = ticker;

      this.baseWaitingTimeMs = baseWaitingTimeMs;
      this.waitingTimeMs = baseWaitingTimeMs;
      this.expiryTimeMs = ticker.read() + baseWaitingTimeMs;
      this.retryBackoffMaxMs = retryBackoffMaxMs;
    }

    public long nextRestartTimeMs() {
      return expiryTimeMs;
    }

    public int getNumRetries() {
      return numRetries;
    }

    public void restart() {
      numRetries++;

      LOG.info("Restarting query {} (attempt #{})", queryId, numRetries);

      // Stop the queryId using the current QueryMetadata
      ksqlEngine.getPersistentQuery(queryId).ifPresent(q -> q.stop());

      // Reset the internal KafkaStreams query. This creates a new QueryMetadata.
      ksqlEngine.resetQuery(queryId);

      // Start the queryId using the new QueryMetadata created during the reset.
      ksqlEngine.getPersistentQuery(queryId).ifPresent(q -> q.start());

      this.waitingTimeMs = getWaitingTimeMs();
      this.expiryTimeMs = ticker.read() + waitingTimeMs;

      LOG.info(
          "Query {} restarted. If the error persists, the query will be restarted again in {} ms",
          queryId, waitingTimeMs);
    }

    private long getWaitingTimeMs() {
      if ((waitingTimeMs * 2) < retryBackoffMaxMs) {
        return waitingTimeMs * 2;
      } else {
        // Add some fuzz amount to the retryBackoffMaxMs to avoid several queries hitting
        // the max. amount are not restarted at the same time
        final int fuzz = random.nextInt((int) baseWaitingTimeMs / 2);
        return retryBackoffMaxMs + fuzz;
      }
    }
  }
}
