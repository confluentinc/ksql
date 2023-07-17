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

public class Quadruple<T1, T2, T3, T4> {

  public final T1 first;
  public final T2 second;
  public final T3 third;
  public final T4 fourth;


  public static <T1, T2, T3, T4> Quadruple<T1, T2, T3, T4> of(final T1 first,
                                                              final T2 second,
                                                              final T3 third,
                                                              final T4 fourth) {

    return new Quadruple<>(first, second, third, fourth);
  }

  public Quadruple(final T1 first, final T2 second, final T3 third, final T4 fourth) {
    this.first = first;
    this.second = second;
    this.third = third;
    this.fourth = fourth;
  }

  public T1 getFirst() {
    return first;
  }

  public T2 getSecond() {
    return second;
  }

  public T3 getThird() {
    return third;
  }

  public T4 getFourth() {
    return fourth;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Quadruple<?, ?, ?, ?> quadruple = (Quadruple<?, ?, ?, ?>) o;
    return Objects.equals(first, quadruple.first)
            && Objects.equals(second, quadruple.second)
            && Objects.equals(third, quadruple.third)
            && Objects.equals(fourth, quadruple.fourth);
  }

  @Override
  public int hashCode() {
    return Objects.hash(first, second, third, fourth);
  }

  @Override
  public String toString() {
    return "Quadruple{"
            + "first=" + first
            + ", second=" + second
            + ", third=" + third
            + ", fourth=" + fourth
            + '}';
  }
}