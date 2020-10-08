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
import java.util.HashMap;
import java.util.Iterator;
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
  private final long statusRunningThresholdMs;
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
        ksqlConfig.getInt(KsqlConfig.KSQL_QUERY_STATUS_RUNNING_THRESHOLD_SECS) * 1000,
        CURRENT_TIME_MILLIS_TICKER
    );
  }

  @VisibleForTesting
  QueryMonitor(
      final KsqlEngine ksqlEngine,
      final ExecutorService executor,
      final long retryBackoffInitialMs,
      final long retryBackoffMaxMs,
      final long statusRunningThresholdMs,
      final Ticker ticker
  ) {
    this.retryBackoffInitialMs = retryBackoffInitialMs;
    this.retryBackoffMaxMs = retryBackoffMaxMs;
    this.statusRunningThresholdMs = statusRunningThresholdMs;
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
    final Iterator<Map.Entry<QueryId, RetryEvent>> it = queriesRetries.entrySet().iterator();
    while (it.hasNext()) {
      final Map.Entry<QueryId, RetryEvent> mapEntry = it.next();

      final QueryId queryId = mapEntry.getKey();
      final RetryEvent retryEvent = mapEntry.getValue();
      final Optional<PersistentQueryMetadata> query = ksqlEngine.getPersistentQuery(queryId);

      // Query was terminated manually if no present
      if (!query.isPresent()) {
        LOG.debug("Query {} was manually terminated. Removing from query retry monitor.", queryId);
        it.remove();
      } else {
        final PersistentQueryMetadata queryMetadata = query.get();
        switch (queryMetadata.getState()) {
          case ERROR:
            if (now >= retryEvent.nextRestartTimeMs()) {
              retryEvent.restart(queryMetadata);
            }
            break;
          case RUNNING:
          case REBALANCING:
            if (queryMetadata.uptime() >= statusRunningThresholdMs) {
              // Clean the errors queue & delete the query from future retries now the query is
              // healthy and has been running after some threshold time
              LOG.info("Query {} has been running for more than {} seconds. "
                      + "Marking query as healthy.", queryId, statusRunningThresholdMs * 1000);
              queryMetadata.clearErrors();
              it.remove();
            }
            break;
          case CREATED:
            // Do nothing.
            continue;
          default:
            // Stop attempting restarts for any other status. Either the query is pending
            // a shutdown or other status that we do not track.
            LOG.debug("Query {} is in status {}. Removing from query retry monitor.",
                queryId, queryMetadata.getState());
            it.remove();
            break;
        }
      }
    }
  }

  private RetryEvent newRetryEvent(final QueryId queryId) {
    return new RetryEvent(queryId, retryBackoffInitialMs, retryBackoffMaxMs, ticker);
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
    private final QueryId queryId;
    private final Ticker ticker;

    private int numRetries = 0;
    private long waitingTimeMs;
    private long expiryTimeMs;
    private long retryBackoffMaxMs;
    private long baseWaitingTimeMs;

    RetryEvent(
        final QueryId queryId,
        final long baseWaitingTimeMs,
        final long retryBackoffMaxMs,
        final Ticker ticker
    ) {
      this.queryId = queryId;
      this.ticker = ticker;

      final long now = ticker.read();

      this.baseWaitingTimeMs = baseWaitingTimeMs;
      this.waitingTimeMs = baseWaitingTimeMs;
      this.retryBackoffMaxMs = retryBackoffMaxMs;
      this.expiryTimeMs = now + baseWaitingTimeMs;
    }

    public long nextRestartTimeMs() {
      return expiryTimeMs;
    }

    public int getNumRetries() {
      return numRetries;
    }

    public void restart(final PersistentQueryMetadata query) {
      numRetries++;

      LOG.info("Restarting query {} (attempt #{})", queryId, numRetries);
      try {
        query.restart();
      } catch (final Exception e) {
        LOG.warn("Failed restarting query {}. Error = {}", queryId, e.getMessage());
      }

      final long now = ticker.read();

      this.waitingTimeMs = getWaitingTimeMs();

      // Math.max() prevents overflow if now is Long.MAX_VALUE (found just in tests)
      this.expiryTimeMs = Math.max(now, now + waitingTimeMs);
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
