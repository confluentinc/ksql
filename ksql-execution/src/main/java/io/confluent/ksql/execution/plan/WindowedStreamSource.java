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

import com.fasterxml.jackson.annotation.JsonProperty;
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
      @JsonProperty(value = "properties", required = true) ExecutionStepProperties properties,
      @JsonProperty(value = "topicName", required = true) String topicName,
      @JsonProperty(value = "formats", required = true) Formats formats,
      @JsonProperty(value = "timestampPolicy", required = true)
      TimestampExtractionPolicy timestampPolicy,
      @JsonProperty(value = "timestampIndex", required = true) int timestampIndex,
      @JsonProperty(value = "offsetReset", required = true)
      Optional<AutoOffsetReset> offsetReset,
      @JsonProperty(value = "sourceSchema", required = true) LogicalSchema sourceSchema,
      @JsonProperty(value = "alias", required = true) SourceName alias) {
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
  public KStreamHolder<Windowed<Struct>> build(PlanBuilder builder) {
    return builder.visitWindowedStreamSource(this);
  }
}
