/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.query.CompletionHandler;
import io.confluent.ksql.query.LimitHandler;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

/**
 * Interface common to types of push queries.  Used for the various endpoints utilizing push
 * queries.
 */
public interface PushQueryMetadata {
  void start();

  void close();

  boolean isRunning();

  BlockingRowQueue getRowQueue();

  void setLimitHandler(LimitHandler limitHandler);

  void setCompletionHandler(CompletionHandler completionHandler);

  void setUncaughtExceptionHandler(StreamsUncaughtExceptionHandler handler);

  LogicalSchema getLogicalSchema();

  QueryId getQueryId();

  ResultType getResultType();

  enum ResultType {
    STREAM,
    TABLE,
    WINDOWED_TABLE
  }
}
