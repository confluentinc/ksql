package io.confluent.ksql.rest.integration;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.reactive.BaseSubscriber;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.vertx.core.Context;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.reactivestreams.Subscription;

/**
 * A useful subscriber for testing. It calls back on the given futures when the header is received
 * as well as the complete set of rows. Also, the method {@code getRows} can be called to poll the
 * currently received rows in a test.
 */
public class QueryStreamSubscriber extends BaseSubscriber<StreamedRow> {

  private final CompletableFuture<List<StreamedRow>> future;
  private final CompletableFuture<StreamedRow> header;
  private boolean closed;
  private List<StreamedRow> rows = new ArrayList<>();

  public QueryStreamSubscriber(
      final Context context,
      final CompletableFuture<List<StreamedRow>> future,
      final CompletableFuture<StreamedRow> header
  ) {
    super(context);
    this.future = Objects.requireNonNull(future);
    this.header = Objects.requireNonNull(header);
  }

  @Override
  protected void afterSubscribe(final Subscription subscription) {
    makeRequest(1);
  }

  @Override
  protected synchronized void handleValue(final StreamedRow row) {
    if (closed) {
      return;
    }
    rows.add(row);
    if (row.isTerminal()) {
      future.complete(rows);
      return;
    }
    if (row.getHeader().isPresent()) {
      header.complete(row);
    }
    makeRequest(1);
  }

  @Override
  protected void handleComplete() {
    future.complete(rows);
  }

  @Override
  protected void handleError(final Throwable t) {
    header.completeExceptionally(t);
    future.completeExceptionally(t);
  }

  public void close() {
    closed = true;
    context.runOnContext(v -> cancel());
  }

  public synchronized List<StreamedRow> getRows() {
    return ImmutableList.copyOf(rows);
  }

  public synchronized List<StreamedRow> getUniqueRows() {
    return rows.stream()
        .distinct()
        .collect(ImmutableList.toImmutableList());
  }
}