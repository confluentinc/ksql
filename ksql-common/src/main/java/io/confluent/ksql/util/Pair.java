/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

import java.util.Objects;

public class Pair<T1, T2> {

  public final T1 left;
  public final T2 right;

  public static <L, R> Pair<L, R> of(final L left, final R right) {
    return new Pair<>(left, right);
  }

  public Pair(final T1 left, final T2 right) {
    this.left = left;
    this.right = right;
  }

  public T1 getLeft() {
    return left;
  }

  public T2 getRight() {
    return right;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Pair<?, ?> pair = (Pair<?, ?>) o;
    return Objects.equals(left, pair.left)
        && Objects.equals(right, pair.right);
  }

  @Override
  public int hashCode() {
    return Objects.hash(left, right);
  }

  @Override
  public String toString() {
    return "Pair{"
        + "left=" + left
        + ", right=" + right
        + '}';
  }
}
