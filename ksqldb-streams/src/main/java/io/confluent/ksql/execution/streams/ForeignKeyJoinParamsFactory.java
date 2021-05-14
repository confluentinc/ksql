/*
 * Copyright 2021 Confluent Inc.
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

import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;
import java.util.Optional;

public final class ForeignKeyJoinParamsFactory {

  private ForeignKeyJoinParamsFactory() {
  }

  public static <KRightT> ForeignKeyJoinParams<KRightT> create(
      final ColumnName leftJoinColumnName,
      final LogicalSchema leftSchema,
      final LogicalSchema rightSchema
  ) {
    if (rightSchema.key().size() != 1) {
      throw new IllegalStateException("rightSchema must have single column key");
    }
    return new ForeignKeyJoinParams<>(
        createKeyExtractor(leftSchema, leftJoinColumnName),
        new KsqlValueJoiner(leftSchema.value().size(), rightSchema.value().size(), 0),
        createSchema(leftSchema, rightSchema)
    );
  }

  public static LogicalSchema createSchema(
      final LogicalSchema leftSchema,
      final LogicalSchema rightSchema
  ) {
    final Builder builder = LogicalSchema.builder()
        .keyColumns(leftSchema.key())
        .valueColumns(leftSchema.value())
        .valueColumns(rightSchema.value());

    return builder.build();
  }

  private static <KRightT> KsqlKeyExtractor<KRightT> createKeyExtractor(
      final LogicalSchema leftSchema,
      final ColumnName leftJoinColumnName) {

    final Optional<Column> leftJoinColumn = leftSchema.findValueColumn(leftJoinColumnName);
    if (!leftJoinColumn.isPresent()) {
      throw new IllegalStateException("Could not find join column in left input table.");
    }

    return new KsqlKeyExtractor<>(leftJoinColumn.get().index());
  }
}
