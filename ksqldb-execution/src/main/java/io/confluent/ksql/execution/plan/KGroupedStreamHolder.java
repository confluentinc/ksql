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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Objects;
import org.apache.kafka.streams.kstream.KGroupedStream;

@Immutable
public final class KGroupedStreamHolder {

  private final KGroupedStream<GenericKey, GenericRow> groupedStream;
  private final LogicalSchema schema;

  private KGroupedStreamHolder(
      final KGroupedStream<GenericKey, GenericRow> groupedStream,
      final LogicalSchema schema) {
    this.groupedStream = Objects.requireNonNull(groupedStream, "groupedStream");
    this.schema = Objects.requireNonNull(schema, "schema");
  }

  public static KGroupedStreamHolder of(
      final KGroupedStream<GenericKey, GenericRow> groupedStream,
      final LogicalSchema schema) {
    return new KGroupedStreamHolder(groupedStream, schema);
  }

  public LogicalSchema getSchema() {
    return schema;
  }

  public KGroupedStream<GenericKey, GenericRow> getGroupedStream() {
    return groupedStream;
  }
}
