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

import com.google.common.collect.Streams;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import java.util.List;

public final class JoinParamsFactory {

  private JoinParamsFactory() {
  }

  public static JoinParams create(
      final ColumnName keyColName,
      final LogicalSchema leftSchema,
      final LogicalSchema rightSchema
  ) {
    final boolean appendKey = neitherContain(keyColName, leftSchema, rightSchema);
    return new JoinParams(
        new KsqlValueJoiner(leftSchema.value().size(), rightSchema.value().size(), appendKey ? 1 : 0
        ),
        createSchema(keyColName, leftSchema, rightSchema)
    );
  }

  public static LogicalSchema createSchema(
      final ColumnName keyColName,
      final LogicalSchema leftSchema,
      final LogicalSchema rightSchema
  ) {
    final SqlType keyType = throwOnKeyMismatch(leftSchema, rightSchema);

    final Builder builder = LogicalSchema.builder()
        .keyColumn(keyColName, keyType)
        .valueColumns(leftSchema.value())
        .valueColumns(rightSchema.value());

    if (neitherContain(keyColName, leftSchema, rightSchema)) {
      // Append key to value so its accessible during processing:
      builder.valueColumn(keyColName, keyType);
    }

    return builder.build();
  }

  private static SqlType throwOnKeyMismatch(
      final LogicalSchema leftSchema,
      final LogicalSchema rightSchema
  ) {
    final List<Column> leftKeyCols = leftSchema.key();
    final List<Column> rightKeyCols = rightSchema.key();
    if (leftKeyCols.size() != 1 || rightKeyCols.size() != 1) {
      throw new UnsupportedOperationException("Multi-key joins not supported");
    }

    final Column leftKey = leftKeyCols.get(0);
    final Column rightKey = rightKeyCols.get(0);

    if (!leftKey.type().equals(rightKey.type())) {
      throw new KsqlException("Invalid join. Key types differ: "
          + leftKey.type() + " vs " + rightKey.type());
    }

    return leftKey.type();
  }

  @SuppressWarnings("UnstableApiUsage")
  private static boolean neitherContain(
      final ColumnName keyColName,
      final LogicalSchema leftSchema,
      final LogicalSchema rightSchema
  ) {
    return Streams.concat(leftSchema.value().stream(), rightSchema.value().stream())
        .noneMatch(c -> c.name().equals(keyColName));
  }
}
