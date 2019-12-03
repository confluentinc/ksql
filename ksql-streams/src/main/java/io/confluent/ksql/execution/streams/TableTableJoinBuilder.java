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
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.TableTableJoin;
import org.apache.kafka.streams.kstream.KTable;

public final class TableTableJoinBuilder {
  private TableTableJoinBuilder() {
  }

  public static <K> KTableHolder<K> build(
      final KTableHolder<K> left,
      final KTableHolder<K> right,
      final TableTableJoin<K> join
  ) {
    final KsqlValueJoiner joiner = new KsqlValueJoiner(left.getSchema(), right.getSchema());

    final KTable<K, GenericRow> result;
    switch (join.getJoinType()) {
      case LEFT:
        result = left.getTable().leftJoin(right.getTable(), joiner);
        break;
      case INNER:
        result = left.getTable().join(right.getTable(), joiner);
        break;
      case OUTER:
        result = left.getTable().outerJoin(right.getTable(), joiner);
        break;
      default:
        throw new IllegalStateException("invalid join type");
    }

    return left.withTable(result, join.getSchema());
  }
}