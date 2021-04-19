package io.confluent.ksql.util;

import io.confluent.ksql.physical.scalable_push.PushQueryQueuePopulator;
import io.confluent.ksql.physical.scalable_push.PushRouting.PushConnectionsHandle;
import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.query.LimitHandler;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.TransientQueryMetadata.ResultType;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

public class ScalablePushQueryMetadata implements PushQueryMetadata {

  private volatile boolean closed = false;
  private final LogicalSchema logicalSchema;
  private final QueryId queryId;
  private final BlockingRowQueue rowQueue;
  private final ResultType resultType;
  private final PushQueryQueuePopulator pushQueryQueuePopulator;

  private final AtomicReference<PushConnectionsHandle> handleRef = new AtomicReference<>();

  public ScalablePushQueryMetadata(
      final LogicalSchema logicalSchema,
      final QueryId queryId,
      final BlockingRowQueue blockingRowQueue,
      final ResultType resultType,
      final PushQueryQueuePopulator pushQueryQueuePopulator
  ) {
    this.logicalSchema = logicalSchema;
    this.queryId = queryId;
    this.rowQueue = blockingRowQueue;
    this.resultType = resultType;
    this.pushQueryQueuePopulator = pushQueryQueuePopulator;
  }

  @Override
  public void start() {
    pushQueryQueuePopulator.run().thenApply(handle -> {
      handleRef.set(handle);
      return null;
    });
  }

  @Override
  public void close() {
    rowQueue.close();
    final PushConnectionsHandle handle = handleRef.get();
    if (handle != null) {
      handle.close();
    }
    closed = true;
  }

  @Override
  public boolean isRunning() {
    return !closed;
  }

  @Override
  public BlockingRowQueue getRowQueue() {
    return rowQueue;
  }

  @Override
  public void setLimitHandler(LimitHandler limitHandler) {
    rowQueue.setLimitHandler(limitHandler);
  }

  @Override
  public void setUncaughtExceptionHandler(StreamsUncaughtExceptionHandler handler) {
    // We don't do anything special here since the persistent query handles its own errors
  }

  @Override
  public LogicalSchema getLogicalSchema() {
    return logicalSchema;
  }

  @Override
  public QueryId getQueryId() {
    return queryId;
  }

  @Override
  public ResultType getResultType() {
    return resultType;
  }
}
