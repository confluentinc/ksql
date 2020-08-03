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
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

  private static final int SHUTDOWN_TIMEOUT_MS = 5000;

  private final Ticker ticker;
  private final long retryBackoffInitialMs;
  private final long retryBackoffMaxMs;
  private final KsqlEngine ksqlEngine;
  private final ExecutorService executor;
  private final Map<QueryId, RetryEvent> queriesRetries = new HashMap<>();

  private volatile boolean closed = false;

  public QueryMonitor(final KsqlConfig ksqlConfig, final KsqlEngine ksqlEngine) {
    this(
        ksqlEngine,
        Executors.newSingleThreadExecutor(r -> new Thread(r, QueryMonitor.class.getName())),
        ksqlConfig.getLong(KsqlConfig.KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS),
        ksqlConfig.getLong(KsqlConfig.KSQL_QUERY_RETRY_BACKOFF_MAX_MS),
        CURRENT_TIME_MILLIS_TICKER
    );
  }

  @VisibleForTesting
  QueryMonitor(
      final KsqlEngine ksqlEngine,
      final ExecutorService executor,
      final long retryBackoffInitialMs,
      final long retryBackoffMaxMs,
      final Ticker ticker
  ) {
    this.retryBackoffInitialMs = retryBackoffInitialMs;
    this.retryBackoffMaxMs = retryBackoffMaxMs;
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
    // Collect a list of new queries in ERROR state
    ksqlEngine.getPersistentQueries().stream()
        .filter(QueryMetadata::isError)
        .filter(query -> !queriesRetries.containsKey(query.getQueryId()))
        .map(QueryMetadata::getQueryId)
        .forEach(queryId -> queriesRetries.put(queryId, newRetryEvent(queryId)));

    maybeRestartQueries();
  }

  private void maybeRestartQueries() {
    final long now = ticker.read();

    // Restart queries that has passed the waiting timeout
    final List<QueryId> deleteRetryEvents = new ArrayList<>();
    queriesRetries.entrySet().stream()
        .forEach(mapEntry -> {
          final QueryId queryId = mapEntry.getKey();
          final RetryEvent retryEvent = mapEntry.getValue();
          final Optional<PersistentQueryMetadata> query = ksqlEngine.getPersistentQuery(queryId);

          // Query was terminated manually if no present
          if (!query.isPresent()) {
            deleteRetryEvents.add(queryId);
          } else if (query.get().isError() && now > retryEvent.nextRestartTimeMs()) {
            // Retry again if it's still in ERROR state
            retryEvent.restart();
          } else if (now > retryEvent.queryHealthyTime()) {
            // Clean the errors queue & delete the query from future retries now the query is
            // healthy
            query.ifPresent(QueryMetadata::clearErrors);
            deleteRetryEvents.add(queryId);
          }
        });

    deleteRetryEvents.stream().forEach(queriesRetries::remove);
  }

  private RetryEvent newRetryEvent(final QueryId queryId) {
    return new RetryEvent(ksqlEngine, queryId, retryBackoffInitialMs, retryBackoffMaxMs, ticker);
  }

  private class Runner implements Runnable {
    @Override
    public void run() {
      LOG.info("KSQL query monitor started.");

      while (!closed) {
        restartFailedQueries();

        try {
          Thread.sleep(500);
        } catch (final InterruptedException e) {
          LOG.info("QueryMonitor sleep thread interrupted. Terminating QueryMonitor. Error = {}",
              e.getMessage());
          break;
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

    public long queryHealthyTime() {
      // Return same value as nextRestartTimeMs for now. QueryHealthyTime may be configured in
      // the future to return a time when a running query is considered healthy.
      return expiryTimeMs;
    }

    public void restart() {
      numRetries++;

      LOG.info("Restarting query {} (attempt #{})", queryId, numRetries);
      ksqlEngine.getPersistentQuery(queryId).ifPresent(q -> {
        try {
          q.restart();
        } catch (final Exception e) {
          LOG.warn("Failed restarting query {}. Error = {}", queryId, e.getMessage());
        }
      });

      this.waitingTimeMs = getWaitingTimeMs();
      this.expiryTimeMs = ticker.read() + waitingTimeMs;
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
