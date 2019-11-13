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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Objects;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KGroupedStream;

public final class KGroupedStreamHolder {
  final KGroupedStream<Struct, GenericRow> groupedStream;
  final LogicalSchema schema;

  private KGroupedStreamHolder(
      KGroupedStream<Struct, GenericRow> groupedStream,
      LogicalSchema schema) {
    this.groupedStream = Objects.requireNonNull(groupedStream, "groupedStream");
    this.schema = Objects.requireNonNull(schema, "schema");
  }

  public static KGroupedStreamHolder of(
      KGroupedStream<Struct, GenericRow> groupedStream,
      LogicalSchema schema) {
    return new KGroupedStreamHolder(groupedStream, schema);
  }

  public LogicalSchema getSchema() {
    return schema;
  }

  public KGroupedStream<Struct, GenericRow> getGroupedStream() {
    return groupedStream;
  }
}
