package io.confluent.ksql.util;

import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.query.LimitHandler;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.TransientQueryMetadata.ResultType;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

public interface PushQueryMetadata {
  void start();
  void close();
  boolean isRunning();
  BlockingRowQueue getRowQueue();

  void setLimitHandler(final LimitHandler limitHandler);
  void setUncaughtExceptionHandler(final StreamsUncaughtExceptionHandler handler);
  LogicalSchema getLogicalSchema();
  QueryId getQueryId();
  ResultType getResultType();
}
