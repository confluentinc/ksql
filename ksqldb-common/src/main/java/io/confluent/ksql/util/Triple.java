/*
 * Copyright 2022 Confluent Inc.
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

public class Triple<T1, T2, T3> {

  public final T1 left;
  public final T2 middle;
  public final T3 right;


  public static <L, M, R> Triple<L, M, R> of(final L left, final M middle, final R right) {
    return new Triple<>(left, middle, right);
  }

  public Triple(final T1 left, final T2 middle, final T3 right) {
    this.left = left;
    this.middle = middle;
    this.right = right;
  }

  public T1 getLeft() {
    return left;
  }

  public T2 getMiddle() {
    return middle;
  }

  public T3 getRight() {
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
    final Triple<?, ?, ?> triple = (Triple<?, ?, ?>) o;
    return Objects.equals(left, triple.left)
            && Objects.equals(middle, triple.middle)
            && Objects.equals(right, triple.right);
  }

  @Override
  public int hashCode() {
    return Objects.hash(left, middle, right);
  }

  @Override
  public String toString() {
    return "Triple{"
            + "left=" + left
            + ", middle=" + middle
            + ", right=" + right
            + '}';
  }
}