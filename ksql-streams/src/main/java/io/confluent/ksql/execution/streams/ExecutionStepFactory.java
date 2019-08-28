/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.streams;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.DefaultExecutionStepProperties;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.LogicalSchemaWithMetaAndKeyFields;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Windowed;

public final class ExecutionStepFactory {
  private ExecutionStepFactory() {
  }

  public static StreamSource<KStream<Windowed<Struct>, GenericRow>> streamSourceWindowed(
      final QueryContext queryContext,
      final LogicalSchemaWithMetaAndKeyFields schema,
      final String topicName,
      final Formats formats,
      final TimestampExtractionPolicy timestampPolicy,
      final int timestampIndex,
      final Optional<AutoOffsetReset> offsetReset
  ) {
    return new StreamSource<>(
        new DefaultExecutionStepProperties(
            queryContext.toString(),
            schema.getSchema(),
            queryContext),
        topicName,
        formats,
        timestampPolicy,
        timestampIndex,
        offsetReset,
        schema.getOriginalSchema(),
        StreamSourceBuilder::buildWindowed
    );
  }

  public static StreamSource<KStream<Struct, GenericRow>> streamSource(
      final QueryContext queryContext,
      final LogicalSchemaWithMetaAndKeyFields schema,
      final String topicName,
      final Formats formats,
      final TimestampExtractionPolicy timestampPolicy,
      final int timestampIndex,
      final Optional<AutoOffsetReset> offsetReset
  ) {
    return new StreamSource<>(
        new DefaultExecutionStepProperties(
            queryContext.toString(),
            schema.getSchema(),
            queryContext),
        topicName,
        formats,
        timestampPolicy,
        timestampIndex,
        offsetReset,
        schema.getOriginalSchema(),
        StreamSourceBuilder::buildUnwindowed
    );
  }
}
