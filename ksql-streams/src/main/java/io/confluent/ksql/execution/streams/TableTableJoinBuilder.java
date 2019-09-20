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
import io.confluent.ksql.execution.plan.TableTableJoin;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import org.apache.kafka.streams.kstream.KTable;

public final class TableTableJoinBuilder {
  private TableTableJoinBuilder() {
  }

  public static <K> KTable<K, GenericRow> build(
      final KTable<K, GenericRow> left,
      final KTable<K, GenericRow> right,
      final TableTableJoin join) {
    final LogicalSchema leftSchema = join.getLeft().getProperties().getSchema();
    final LogicalSchema rightSchema = join.getRight().getProperties().getSchema();
    final KsqlValueJoiner joiner = new KsqlValueJoiner(leftSchema, rightSchema);
    switch (join.getJoinType()) {
      case LEFT:
        return left.leftJoin(right, joiner);
      case INNER:
        return left.join(right, joiner);
      case OUTER:
        return left.outerJoin(right, joiner);
      default:
        throw new IllegalStateException("invalid join type");
    }
  }
}