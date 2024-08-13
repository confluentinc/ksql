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

import static io.confluent.ksql.execution.plan.JoinType.RIGHT;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.TableTableJoin;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import org.apache.kafka.streams.kstream.KTable;

public final class TableTableJoinBuilder {

  private TableTableJoinBuilder() {
  }

  public static <K> KTableHolder<K> build(
      final KTableHolder<K> left,
      final KTableHolder<K> right,
      final TableTableJoin<K> join
  ) {
    final LogicalSchema leftSchema;
    final LogicalSchema rightSchema;

    if (join.getJoinType().equals(RIGHT)) {
      leftSchema = right.getSchema();
      rightSchema = left.getSchema();
    } else {
      leftSchema = left.getSchema();
      rightSchema = right.getSchema();
    }

    final JoinParams joinParams = JoinParamsFactory
        .create(join.getKeyColName(), leftSchema, rightSchema);

    final KTable<K, GenericRow> result;
    switch (join.getJoinType()) {
      case INNER:
        result = left.getTable().join(right.getTable(), joinParams.getJoiner());
        break;
      case LEFT:
        result = left.getTable().leftJoin(right.getTable(), joinParams.getJoiner());
        break;
      case RIGHT:
        result = right.getTable().leftJoin(left.getTable(), joinParams.getJoiner());
        break;
      case OUTER:
        result = left.getTable().outerJoin(right.getTable(), joinParams.getJoiner());
        break;
      default:
        throw new IllegalStateException("invalid join type: " + join.getJoinType());
    }

    return KTableHolder.unmaterialized(
            result,
            joinParams.getSchema(),
            left.getExecutionKeyFactory());
  }
}