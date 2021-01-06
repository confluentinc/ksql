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

import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.StreamSink;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;

public final class StreamSinkBuilder {
  private StreamSinkBuilder() {
  }

  public static <K> void build(
      final KStreamHolder<K> stream,
      final StreamSink<K> streamSink,
      final RuntimeBuildContext buildContext) {
    SinkBuilder.build(
        stream.getSchema(),
        streamSink.getFormats(),
        streamSink.getTimestampColumn(),
        streamSink.getTopicName(),
        stream.getStream(),
        stream.getExecutionKeyFactory(),
        streamSink.getProperties().getQueryContext(),
        buildContext
    );
  }
}
