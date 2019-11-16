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

import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.SchemaUtil;

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
    final LogicalSchema.Builder joinSchema = LogicalSchema.builder();

    // Hard-wire for now, until we support custom type/name of key fields:
    joinSchema.keyColumn(SchemaUtil.ROWKEY_NAME, SqlTypes.STRING);

    joinSchema.valueColumns(leftSchema.value());

    joinSchema.valueColumns(rightSchema.value());

    return joinSchema.build();
  }
}
