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
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.Topology.AutoOffsetReset;

@Immutable
public final class TableSource extends AbstractStreamSource<KTableHolder<Struct>> {

  public TableSource(
      @JsonProperty(value = "properties", required = true) final ExecutionStepProperties properties,
      @JsonProperty(value = "topicName", required = true) final String topicName,
      @JsonProperty(value = "formats", required = true) final Formats formats,
      @JsonProperty("timestampColumn") final Optional<TimestampColumn> timestampColumn,
      @JsonProperty("offsetReset") final Optional<AutoOffsetReset> offsetReset,
      @JsonProperty(value = "sourceSchema", required = true) final LogicalSchema sourceSchema,
      @JsonProperty(value = "alias", required = true) final SourceName alias
  ) {
    super(
        properties,
        topicName,
        formats,
        timestampColumn,
        offsetReset,
        sourceSchema,
        alias
    );
  }

  @Override
  public KTableHolder<Struct> build(PlanBuilder builder) {
    return builder.visitTableSource(this);
  }
}
