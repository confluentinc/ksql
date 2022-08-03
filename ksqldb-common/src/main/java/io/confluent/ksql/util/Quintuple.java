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

public class Quintuple<T1, T2, T3, T4, T5> {

  public final T1 first;
  public final T2 second;
  public final T3 third;
  public final T4 fourth;
  public final T5 fifth;


  public static <T1, T2, T3, T4, T5> Quintuple<T1, T2, T3, T4, T5> of(final T1 first,
                                                                      final T2 second,
                                                                      final T3 third,
                                                                      final T4 fourth,
                                                                      final T5 fifth) {
    return new Quintuple<>(first, second, third, fourth, fifth);
  }

  public Quintuple(final T1 first,
                   final T2 second,
                   final T3 third,
                   final T4 fourth,
                   final T5 fifth) {
    this.first = first;
    this.second = second;
    this.third = third;
    this.fourth = fourth;
    this.fifth = fifth;
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

  public T5 getFifth() {
    return fifth;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Quintuple<?, ?, ?, ?, ?> quintuple = (Quintuple<?, ?, ?, ?, ?>) o;
    return Objects.equals(first, quintuple.first)
            && Objects.equals(second, quintuple.second)
            && Objects.equals(third, quintuple.third)
            && Objects.equals(fourth, quintuple.fourth)
            && Objects.equals(fifth, quintuple.fifth);
  }

  @Override
  public int hashCode() {
    return Objects.hash(first, second, third, fourth, fifth);
  }

  @Override
  public String toString() {
    return "Quintuple{"
            + "first=" + first
            + ", second=" + second
            + ", third=" + third
            + ", fourth=" + fourth
            + ", fifth=" + fifth
            + '}';
  }
}