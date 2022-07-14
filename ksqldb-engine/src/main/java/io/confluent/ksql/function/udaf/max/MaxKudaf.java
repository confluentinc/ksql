/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.function.udaf.max;

import io.confluent.ksql.function.udaf.BaseComparableKudaf;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.util.KsqlConstants;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

@UdafDescription(
        name = "MAX",
        description = "Computes the maximum value for a key.",
        author = KsqlConstants.CONFLUENT_AUTHOR
)
public class MaxKudaf<T extends Comparable<? super T>> extends BaseComparableKudaf<T> {

  @UdafFactory(description = "Computes the maximum value for an integer key.")
  public static Udaf<Integer, Integer, Integer> createMaxInt() {
    return new MaxKudaf<>();
  }

  @UdafFactory(description = "Computes the maximum value for a long key.")
  public static Udaf<Long, Long, Long> createMaxLong() {
    return new MaxKudaf<>();
  }

  @UdafFactory(description = "Computes the maximum value for a double key.")
  public static Udaf<Double, Double, Double> createMaxDouble() {
    return new MaxKudaf<>();
  }

  @UdafFactory(description = "Computes the maximum value for a decimal key.")
  public static Udaf<BigDecimal, BigDecimal, BigDecimal> createMaxDecimal() {
    return new MaxKudaf<>();
  }

  @UdafFactory(description = "Computes the maximum value for a date key.")
  public static Udaf<Date, Date, Date> createMaxDate() {
    return new MaxKudaf<>();
  }

  @UdafFactory(description = "Computes the maximum value for a time key.")
  public static Udaf<Time, Time, Time> createMaxTime() {
    return new MaxKudaf<>();
  }

  @UdafFactory(description = "Computes the maximum value for a timestamp key.")
  public static Udaf<Timestamp, Timestamp, Timestamp> createMaxTimestamp() {
    return new MaxKudaf<>();
  }

  @UdafFactory(description = "Computes the maximum value for a string key.")
  public static Udaf<String, String, String> createMaxString() {
    return new MaxKudaf<>();
  }

  @UdafFactory(description = "Computes the maximum value for a bytes key.")
  public static Udaf<ByteBuffer, ByteBuffer, ByteBuffer> createMaxBytes() {
    return new MaxKudaf<>();
  }

  public MaxKudaf() {
    super((first, second) -> first.compareTo(second) > 0 ? first : second);
  }
}
