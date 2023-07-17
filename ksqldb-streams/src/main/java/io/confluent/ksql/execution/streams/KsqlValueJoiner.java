/*
 * Copyright 2018 Confluent Inc.
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

import static com.google.common.base.Preconditions.checkArgument;

import io.confluent.ksql.GenericRow;
import java.util.Objects;
import org.apache.kafka.streams.kstream.ValueJoiner;

public final class KsqlValueJoiner implements ValueJoiner<GenericRow, GenericRow, GenericRow> {

  private final int leftCount;
  private final int rightCount;
  private final int additionalCount;

  KsqlValueJoiner(final int leftCount, final int rightCount, final int additionalCount) {
    checkArgument(leftCount >= 0, "leftCount negative: " + leftCount);
    checkArgument(rightCount >= 0, "rightCount negative: " + rightCount);
    checkArgument(additionalCount >= 0, "additionalCount negative: " + additionalCount);

    this.leftCount = leftCount;
    this.rightCount = rightCount;
    this.additionalCount = additionalCount;
  }

  @Override
  public GenericRow apply(final GenericRow left, final GenericRow right) {

    final GenericRow row = new GenericRow(leftCount + rightCount + additionalCount);

    if (left != null) {
      row.appendAll(left.values());
    } else {
      fillWithNulls(row, leftCount);
    }

    if (right != null) {
      row.appendAll(right.values());
    } else {
      fillWithNulls(row, rightCount);
    }

    // Potentially append additional nulls as a holder for a synthetic key columns.
    // These columns are not populated, as they are not accessed, but must be present for the row
    // to match the rows schema.
    fillWithNulls(row, additionalCount);

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
    return Objects.equals(leftCount, that.leftCount)
        && Objects.equals(rightCount, that.rightCount)
        && Objects.equals(additionalCount, that.additionalCount);
  }

  @Override
  public int hashCode() {
    return Objects.hash(leftCount, rightCount, additionalCount);
  }

  @Override
  public String toString() {
    return "KsqlValueJoiner{"
        + "leftCount=" + leftCount
        + ", rightCount=" + rightCount
        + ", additionalCount=" + additionalCount
        + '}';
  }
}
