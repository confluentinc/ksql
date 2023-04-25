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

import io.confluent.ksql.execution.transform.ExpressionEvaluator;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.LogicalSchema.Builder;

public final class ForeignKeyJoinParamsFactory {

  private ForeignKeyJoinParamsFactory() {
  }

  public static <KRightT> ForeignKeyJoinParams<KRightT> create(
      final ExpressionEvaluator expressionEvaluator,
      final LogicalSchema leftSchema,
      final LogicalSchema rightSchema,
      final ProcessingLogger processingLogger
  ) {
    if (rightSchema.key().size() != 1) {
      throw new IllegalStateException("rightSchema must have single column key");
    }
    return new ForeignKeyJoinParams<>(
        new KsqlKeyExtractor<>(expressionEvaluator, processingLogger),
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
}
