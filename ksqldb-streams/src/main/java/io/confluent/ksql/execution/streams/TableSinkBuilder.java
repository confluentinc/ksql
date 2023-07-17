/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.streams;

import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.TableSink;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;

public final class TableSinkBuilder {
  private TableSinkBuilder() {
  }

  public static <K> void build(
      final KTableHolder<K> table,
      final TableSink<K> tableSink,
      final RuntimeBuildContext buildContext) {
    SinkBuilder.build(
        table.getSchema(),
        tableSink.getFormats(),
        tableSink.getTimestampColumn(),
        tableSink.getTopicName(),
        table.getTable().toStream(),
        table.getExecutionKeyFactory(),
        tableSink.getProperties().getQueryContext(),
        buildContext
    );
  }
}
