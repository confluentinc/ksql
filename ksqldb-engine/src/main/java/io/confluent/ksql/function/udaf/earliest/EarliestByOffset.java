/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udaf.earliest;

import static io.confluent.ksql.function.udaf.KudafByOffsetUtils.SEQ_FIELD;
import static io.confluent.ksql.function.udaf.KudafByOffsetUtils.STRUCT_BOOLEAN;
import static io.confluent.ksql.function.udaf.KudafByOffsetUtils.STRUCT_DOUBLE;
import static io.confluent.ksql.function.udaf.KudafByOffsetUtils.STRUCT_INTEGER;
import static io.confluent.ksql.function.udaf.KudafByOffsetUtils.STRUCT_LONG;
import static io.confluent.ksql.function.udaf.KudafByOffsetUtils.STRUCT_STRING;
import static io.confluent.ksql.function.udaf.KudafByOffsetUtils.VAL_FIELD;
import static io.confluent.ksql.function.udaf.KudafByOffsetUtils.compareStructs;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(
    name = "EARLIEST_BY_OFFSET",
    description = EarliestByOffset.DESCRIPTION
)
public final class EarliestByOffset {
  static final String DESCRIPTION =
      "This function returns the oldest value for the column, computed by offset.";

  private EarliestByOffset() {
  }

  static AtomicLong sequence = new AtomicLong();

  @UdafFactory(description = "return the earliest value of an integer column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL INT>")
  public static Udaf<Integer, Struct, Integer> earliestInteger() {
    return earliest(STRUCT_INTEGER);
  }

  @UdafFactory(description = "return the earliest value of an big integer column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL BIGINT>")
  public static Udaf<Long, Struct, Long> earliestLong() {
    return earliest(STRUCT_LONG);
  }

  @UdafFactory(description = "return the earliest value of a double column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL DOUBLE>")
  public static Udaf<Double, Struct, Double> earliestDouble() {
    return earliest(STRUCT_DOUBLE);
  }

  @UdafFactory(description = "return the earliest value of a boolean column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL BOOLEAN>")
  public static Udaf<Boolean, Struct, Boolean> earliestBoolean() {
    return earliest(STRUCT_BOOLEAN);
  }

  @UdafFactory(description = "return the earliest value of a string column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL STRING>")
  public static Udaf<String, Struct, String> earliestString() {
    return earliest(STRUCT_STRING);
  }

  static <T> Struct createStruct(final Schema schema, final T val) {
    final Struct struct = new Struct(schema);
    struct.put(SEQ_FIELD, generateSequence());
    struct.put(VAL_FIELD, val);
    return struct;
  }

  private static long generateSequence() {
    return sequence.getAndIncrement();
  }

  @UdafFactory(description = "Earliest by offset")
  static <T> Udaf<T, Struct, T> earliest(final Schema structSchema) {
    return new Udaf<T, Struct, T>() {

      @Override
      public Struct initialize() {
        return createStruct(structSchema, null);
      }

      @Override
      public Struct aggregate(final T current, final Struct aggregate) {
        if (current == null || aggregate.get(VAL_FIELD) != null) {
          return aggregate;
        } else {
          return createStruct(structSchema, current);
        }
      }

      @Override
      public Struct merge(final Struct aggOne, final Struct aggTwo) {
        // When merging we need some way of evaluating the "earliest' one.
        // We do this by keeping track of the sequence of when it was originally processed
        if (compareStructs(aggOne, aggTwo) < 0) {
          return aggOne;
        } else {
          return aggTwo;
        }
      }

      @Override
      @SuppressWarnings("unchecked")
      public T map(final Struct agg) {
        return (T) agg.get(VAL_FIELD);
      }
    };
  }
}
