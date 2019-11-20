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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.StreamSelectKey;
import io.confluent.ksql.execution.util.StructKeyUtil;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.SchemaUtil;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.KStream;

public final class StreamSelectKeyBuilder {
  private StreamSelectKeyBuilder() {
  }

  public static KStreamHolder<Struct> build(
      final KStreamHolder<?> stream,
      final StreamSelectKey selectKey,
      final KsqlQueryBuilder queryBuilder) {
    final LogicalSchema sourceSchema = stream.getSchema();
    final Column keyColumn = sourceSchema.findValueColumn(selectKey.getFieldName())
        .orElseThrow(IllegalArgumentException::new);
    final int keyIndexInValue = sourceSchema.valueColumnIndex(keyColumn.ref())
        .orElseThrow(IllegalStateException::new);
    final boolean updateRowKey = selectKey.isUpdateRowKey();
    final KStream<?, GenericRow> kstream = stream.getStream();
    final KStream<Struct, GenericRow> rekeyed = kstream
        .filter((key, value) ->
            value != null && extractColumn(sourceSchema, keyIndexInValue, value) != null
        ).selectKey((key, value) ->
            StructKeyUtil.asStructKey(
                extractColumn(sourceSchema, keyIndexInValue, value).toString()
            )
        ).mapValues((key, row) -> {
          if (updateRowKey) {
            final Object rowKey = key.get(key.schema().fields().get(0));
            row.getColumns().set(SchemaUtil.ROWKEY_INDEX, rowKey);
          }
          return row;
        });
    return new KStreamHolder<>(
        rekeyed,
        stream.getSchema(),
        (fmt, schema, ctx) -> queryBuilder.buildKeySerde(fmt.getFormatInfo(), schema, ctx)
    );
  }

  private static Object extractColumn(
      final LogicalSchema schema,
      final int keyIndexInValue,
      final GenericRow value
  ) {
    if (value.getColumns().size() != schema.value().size()) {
      throw new IllegalStateException("Field count mismatch. "
          + "Schema fields: " + schema
          + ", row:" + value);
    }
    return value
        .getColumns()
        .get(keyIndexInValue);
  }
}
