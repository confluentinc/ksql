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

package io.confluent.ksql.execution.plan;

import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Windowed;

public final class WindowedStreamSource
    extends AbstractStreamSource<KStreamHolder<Windowed<Struct>>> {
  public WindowedStreamSource(
      final ExecutionStepProperties properties,
      final String topicName,
      final Formats formats,
      final TimestampExtractionPolicy timestampPolicy,
      final int timestampIndex,
      final Optional<AutoOffsetReset> offsetReset,
      final LogicalSchema sourceSchema,
      final SourceName alias) {
    super(
        properties,
        topicName,
        formats,
        timestampPolicy,
        timestampIndex,
        offsetReset,
        sourceSchema,
        alias
    );
  }

  @Override
  public KStreamHolder<Windowed<Struct>> build(final PlanBuilder builder) {
    return builder.visitWindowedStreamSource(this);
  }
}
