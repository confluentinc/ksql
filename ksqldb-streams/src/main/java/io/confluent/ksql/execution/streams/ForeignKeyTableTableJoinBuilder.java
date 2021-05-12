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
import io.confluent.ksql.execution.plan.ForeignKeyTableTableJoin;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import org.apache.kafka.streams.kstream.KTable;

public final class ForeignKeyTableTableJoinBuilder {

  private ForeignKeyTableTableJoinBuilder() {
  }

  public static <KLeftT, KRightT> KTableHolder<KLeftT> build(
      final KTableHolder<KLeftT> left,
      final KTableHolder<KRightT> right,
      final ForeignKeyTableTableJoin<KLeftT, KRightT> join
  ) {
    final LogicalSchema leftSchema = left.getSchema();
    final LogicalSchema rightSchema = right.getSchema();

    final ForeignKeyJoinParams<KRightT> joinParams = ForeignKeyJoinParamsFactory
        .create(join.getLeftJoinColumnName(), leftSchema, rightSchema);

    final KTable<KLeftT, GenericRow> result;
    switch (join.getJoinType()) {
      case LEFT:
        result = left.getTable().leftJoin(
            right.getTable(),
            joinParams.getKeyExtractor(),
            joinParams.getJoiner()
        );
        break;
      case INNER:
        result = left.getTable().join(
            right.getTable(),
            joinParams.getKeyExtractor(),
            joinParams.getJoiner()
        );
        break;
      default:
        throw new IllegalStateException("invalid join type");
    }

    return KTableHolder.unmaterialized(
            result,
            joinParams.getSchema(),
            left.getExecutionKeyFactory()
    );
  }
}