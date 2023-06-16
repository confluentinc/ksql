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

package io.confluent.ksql.api.server;

import io.confluent.ksql.query.BlockingRowQueue;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.ConsistencyOffsetVector;
import io.confluent.ksql.util.PushQueryMetadata.ResultType;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Handle to a query running in the engine
 */
public interface QueryHandle {

  List<String> getColumnNames();

  List<String> getColumnTypes();

  LogicalSchema getLogicalSchema();

  void start();

  void stop();

  BlockingRowQueue getQueue();

  void onException(Consumer<Throwable> onException);

  QueryId getQueryId();

  Optional<ConsistencyOffsetVector> getConsistencyOffsetVector();

  Optional<ResultType> getResultType();
}
