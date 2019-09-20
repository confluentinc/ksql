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

import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.StreamMapValues;
import io.confluent.ksql.schema.ksql.LogicalSchema;

public final class StreamMapValuesBuilder {
  private StreamMapValuesBuilder() {
  }

  public static <K> KStreamHolder<K> build(
      final KStreamHolder<K> stream,
      final StreamMapValues<K> step,
      final KsqlQueryBuilder queryBuilder) {
    final QueryContext queryContext = step.getProperties().getQueryContext();
    final LogicalSchema sourceSchema = step.getSource().getProperties().getSchema();
    final Selection selection =
        Selection.of(
            queryContext,
            sourceSchema,
            step.getSelectExpressions(),
            queryBuilder.getKsqlConfig(),
            queryBuilder.getFunctionRegistry(),
            queryBuilder.getProcessingLogContext()
        );
    return stream.withStream(
        stream.getStream().mapValues(selection.getMapper())
    );
  }
}
