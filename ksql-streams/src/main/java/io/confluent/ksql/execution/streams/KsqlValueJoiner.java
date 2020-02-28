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
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Objects;
import org.apache.kafka.streams.kstream.ValueJoiner;

public final class KsqlValueJoiner implements ValueJoiner<GenericRow, GenericRow, GenericRow> {
  private final LogicalSchema leftSchema;
  private final LogicalSchema rightSchema;

  KsqlValueJoiner(final LogicalSchema leftSchema, final LogicalSchema rightSchema) {
    this.leftSchema = Objects.requireNonNull(leftSchema, "leftSchema");
    this.rightSchema = Objects.requireNonNull(rightSchema, "rightSchema");
  }

  @Override
  public GenericRow apply(final GenericRow left, final GenericRow right) {
    final GenericRow row = new GenericRow(
        leftSchema.value().size() + rightSchema.value().size()
    );

    if (left != null) {
      row.appendAll(left.values());
    } else {
      fillWithNulls(row, leftSchema.value().size());
    }

    if (right != null) {
      row.appendAll(right.values());
    } else {
      fillWithNulls(row, rightSchema.value().size());
    }

    return row;
  }

  private static void fillWithNulls(final GenericRow row, final int numToFill) {
    for (int i = 0; i < numToFill; ++i) {
      row.append(null);
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KsqlValueJoiner that = (KsqlValueJoiner) o;
    return Objects.equals(leftSchema, that.leftSchema)
        && Objects.equals(rightSchema, that.rightSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(leftSchema, rightSchema);
  }
}
