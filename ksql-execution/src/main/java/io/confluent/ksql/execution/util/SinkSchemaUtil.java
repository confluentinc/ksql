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

package io.confluent.ksql.execution.util;

import com.google.common.collect.Streams;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.ConnectSchema;

public final class SinkSchemaUtil {
  private SinkSchemaUtil() {
  }

  public static LogicalSchema sinkSchema(final ExecutionStep<?> step) {
    final LogicalSchema schema = step.getSources().get(0).getProperties().getSchema();
    return schema.withoutMetaAndKeyColsInValue();
  }

  public static Set<Integer> implicitAndKeyColumnIndexesInValueSchema(
      final ExecutionStep<?> step
  ) {
    final LogicalSchema schema = step.getSources().get(0).getProperties().getSchema();
    final ConnectSchema valueSchema = schema.valueConnectSchema();

    final Stream<Column> cols = Streams.concat(
        schema.metadata().stream(),
        schema.key().stream()
    );

    return cols
        .map(Column::name)
        .map(valueSchema::field)
        .filter(Objects::nonNull)
        .map(org.apache.kafka.connect.data.Field::index)
        .collect(Collectors.toSet());
  }
}
