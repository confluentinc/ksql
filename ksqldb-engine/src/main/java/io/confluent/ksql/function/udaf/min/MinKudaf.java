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

package io.confluent.ksql.function.udaf.min;

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
        name = "MIN",
        description = "Computes the minimum value for a key.",
        author = KsqlConstants.CONFLUENT_AUTHOR
)
public class MinKudaf<T extends Comparable<? super T>> extends BaseComparableKudaf<T> {
  @UdafFactory(description = "Computes the minimum value for an integer key.")
  public static Udaf<Integer, Integer, Integer> createMinInt() {
    return new MinKudaf<>();
  }

  @UdafFactory(description = "Computes the minimum value for a long key.")
  public static Udaf<Long, Long, Long> createMinLong() {
    return new MinKudaf<>();
  }

  @UdafFactory(description = "Computes the minimum value for a double key.")
  public static Udaf<Double, Double, Double> createMinDouble() {
    return new MinKudaf<>();
  }

  @UdafFactory(description = "Computes the minimum value for a decimal key.")
  public static Udaf<BigDecimal, BigDecimal, BigDecimal> createMinDecimal() {
    return new MinKudaf<>();
  }

  @UdafFactory(description = "Computes the minimum value for a date key.")
  public static Udaf<Date, Date, Date> createMinDate() {
    return new MinKudaf<>();
  }

  @UdafFactory(description = "Computes the minimum value for a time key.")
  public static Udaf<Time, Time, Time> createMinTime() {
    return new MinKudaf<>();
  }

  @UdafFactory(description = "Computes the minimum value for a timestamp key.")
  public static Udaf<Timestamp, Timestamp, Timestamp> createMinTimestamp() {
    return new MinKudaf<>();
  }

  @UdafFactory(description = "Computes the minimum value for a string key.")
  public static Udaf<String, String, String> createMinString() {
    return new MinKudaf<>();
  }

  @UdafFactory(description = "Computes the minimum value for a bytes key.")
  public static Udaf<ByteBuffer, ByteBuffer, ByteBuffer> createMinBytes() {
    return new MinKudaf<>();
  }

  public MinKudaf() {
    super((first, second) -> first.compareTo(second) > 0 ? second : first);
  }

}
