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

import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlException;
import java.util.List;

public final class JoinParamsFactory {
  private JoinParamsFactory() {
  }

  public static JoinParams create(
      final LogicalSchema leftSchema,
      final LogicalSchema rightSchema) {
    return new JoinParams(
        new KsqlValueJoiner(leftSchema, rightSchema),
        createSchema(leftSchema, rightSchema)
    );
  }

  public static LogicalSchema createSchema(
      final LogicalSchema leftSchema,
      final LogicalSchema rightSchema
  ) {
    throwOnKeyMismatch(leftSchema, rightSchema);

    return LogicalSchema.builder()
        .keyColumns(leftSchema.withoutAlias().key())
        .valueColumns(leftSchema.value())
        .valueColumns(rightSchema.value())
        .build();
  }

  private static void throwOnKeyMismatch(
      final LogicalSchema leftSchema,
      final LogicalSchema rightSchema
  ) {
    final List<Column> leftCols = leftSchema.key();
    final List<Column> rightCols = rightSchema.key();
    if (leftCols.size() != 1 || rightCols.size() != 1) {
      throw new UnsupportedOperationException("Multi-key joins not supported");
    }

    final Column left = leftCols.get(0);
    final Column right = rightCols.get(0);

    if (!left.type().equals(right.type())) {
      throw new KsqlException("Invalid join. Key types differ: "
          + left.type() + " vs " + right.type());
    }
  }
}
