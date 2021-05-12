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

import static com.google.common.base.Preconditions.checkArgument;

import io.confluent.ksql.GenericRow;
import java.util.Objects;
import java.util.function.Function;

public final class KsqlKeyExtractor<KRightT> implements Function<GenericRow, KRightT> {

  private final int leftJoinColumnIndex;

  KsqlKeyExtractor(final int leftJoinColumnIndex) {
    checkArgument(
        leftJoinColumnIndex >= 0,
        "leftJoinColumnIndex negative: " + leftJoinColumnIndex
    );

    this.leftJoinColumnIndex = leftJoinColumnIndex;
  }

  @SuppressWarnings("unchecked")
  @Override
  public KRightT apply(final GenericRow left) {
    return (KRightT) left.get(leftJoinColumnIndex);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KsqlKeyExtractor that = (KsqlKeyExtractor) o;
    return leftJoinColumnIndex == that.leftJoinColumnIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(leftJoinColumnIndex);
  }

  @Override
  public String toString() {
    return "KsqlKeyExtractor{"
        + "leftJoinColumnIndex=" + leftJoinColumnIndex
        + '}';
  }
}
