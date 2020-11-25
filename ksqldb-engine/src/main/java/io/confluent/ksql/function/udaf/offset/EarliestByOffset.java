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

package io.confluent.ksql.function.udaf.offset;

import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.INTERMEDIATE_STRUCT_COMPARATOR;
import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.STRUCT_BOOLEAN;
import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.STRUCT_DOUBLE;
import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.STRUCT_INTEGER;
import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.STRUCT_LONG;
import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.STRUCT_STRING;
import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.VAL_FIELD;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.util.KsqlConstants;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(
    name = "EARLIEST_BY_OFFSET",
    description = EarliestByOffset.DESCRIPTION,
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public final class EarliestByOffset {

  static final String DESCRIPTION =
      "This function returns the oldest N values for the column, computed by offset.";

  private EarliestByOffset() {
  }

  static AtomicLong sequence = new AtomicLong();

  @UdafFactory(description = "return the earliest value of an integer column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL INT>")
  public static Udaf<Integer, Struct, Integer> earliestInteger() {
    return earliestInteger(true);
  }

  @UdafFactory(description = "return the earliest value of an integer column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL INT>")
  public static Udaf<Integer, Struct, Integer> earliestInteger(final boolean ignoreNulls) {
    return earliest(STRUCT_INTEGER, ignoreNulls);
  }

  @UdafFactory(description = "return the earliest N values of an integer column",
      aggregateSchema = "ARRAY<STRUCT<SEQ BIGINT, VAL INT>>")
  public static Udaf<Integer, List<Struct>, List<Integer>> earliestIntegers(final int earliestN) {
    return earliestIntegers(earliestN, true);
  }

  @UdafFactory(description = "return the earliest N values of an integer column",
      aggregateSchema = "ARRAY<STRUCT<SEQ BIGINT, VAL INT>>")
  public static Udaf<Integer, List<Struct>, List<Integer>> earliestIntegers(
      final int earliestN,
      final boolean ignoreNulls
  ) {
    return earliestN(STRUCT_INTEGER, earliestN, ignoreNulls);
  }

  @UdafFactory(description = "return the earliest value of an big integer column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL BIGINT>")
  public static Udaf<Long, Struct, Long> earliestLong() {
    return earliestLong(true);
  }

  @UdafFactory(description = "return the earliest value of an big integer column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL BIGINT>")
  public static Udaf<Long, Struct, Long> earliestLong(final boolean ignoreNulls) {
    return earliest(STRUCT_LONG, ignoreNulls);
  }

  @UdafFactory(description = "return the earliest N values of an long column",
      aggregateSchema = "ARRAY<STRUCT<SEQ BIGINT, VAL BIGINT>>")
  public static Udaf<Long, List<Struct>, List<Long>> earliestLongs(final int earliestN) {
    return earliestLongs(earliestN, true);
  }

  @UdafFactory(description = "return the earliest N values of an long column",
      aggregateSchema = "ARRAY<STRUCT<SEQ BIGINT, VAL BIGINT>>")
  public static Udaf<Long, List<Struct>, List<Long>> earliestLongs(
      final int earliestN,
      final boolean ignoreNulls
  ) {
    return earliestN(STRUCT_LONG, earliestN, ignoreNulls);
  }

  @UdafFactory(description = "return the earliest value of a double column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL DOUBLE>")
  public static Udaf<Double, Struct, Double> earliestDouble() {
    return earliestDouble(true);
  }

  @UdafFactory(description = "return the earliest value of a double column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL DOUBLE>")
  public static Udaf<Double, Struct, Double> earliestDouble(final boolean ignoreNulls) {
    return earliest(STRUCT_DOUBLE, ignoreNulls);
  }

  @UdafFactory(description = "return the earliest N values of a double column",
      aggregateSchema = "ARRAY<STRUCT<SEQ BIGINT, VAL DOUBLE>>")
  public static Udaf<Double, List<Struct>, List<Double>> earliestDoubles(final int earliestN) {
    return earliestDoubles(earliestN, true);
  }

  @UdafFactory(description = "return the earliest N values of a double column",
      aggregateSchema = "ARRAY<STRUCT<SEQ BIGINT, VAL DOUBLE>>")
  public static Udaf<Double, List<Struct>, List<Double>> earliestDoubles(
      final int earliestN,
      final boolean ignoreNulls
  ) {
    return earliestN(STRUCT_DOUBLE, earliestN, ignoreNulls);
  }

  @UdafFactory(description = "return the earliest value of a boolean column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL BOOLEAN>")
  public static Udaf<Boolean, Struct, Boolean> earliestBoolean() {
    return earliestBoolean(true);
  }

  @UdafFactory(description = "return the earliest value of a boolean column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL BOOLEAN>")
  public static Udaf<Boolean, Struct, Boolean> earliestBoolean(final boolean ignoreNulls) {
    return earliest(STRUCT_BOOLEAN, ignoreNulls);
  }

  @UdafFactory(description = "return the earliest N values of a boolean column",
      aggregateSchema = "ARRAY<STRUCT<SEQ BIGINT, VAL BOOLEAN>>")
  public static Udaf<Boolean, List<Struct>, List<Boolean>> earliestBooleans(final int earliestN) {
    return earliestBooleans(earliestN, true);
  }

  @UdafFactory(description = "return the earliest N values of a boolean column",
      aggregateSchema = "ARRAY<STRUCT<SEQ BIGINT, VAL BOOLEAN>>")
  public static Udaf<Boolean, List<Struct>, List<Boolean>> earliestBooleans(
      final int earliestN,
      final boolean ignoreNulls
  ) {
    return earliestN(STRUCT_BOOLEAN, earliestN, ignoreNulls);
  }

  @UdafFactory(description = "return the earliest value of a string column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL STRING>")
  public static Udaf<String, Struct, String> earliestString() {
    return earliestString(true);
  }

  @UdafFactory(description = "return the earliest value of a string column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL STRING>")
  public static Udaf<String, Struct, String> earliestString(final boolean ignoreNulls) {
    return earliest(STRUCT_STRING, ignoreNulls);
  }

  @UdafFactory(description = "return the earliest N values of a string column",
      aggregateSchema = "ARRAY<STRUCT<SEQ BIGINT, VAL STRING>>")
  public static Udaf<String, List<Struct>, List<String>> earliestStrings(final int earliestN) {
    return earliestStrings(earliestN, true);
  }

  @UdafFactory(description = "return the earliest N values of a string column",
      aggregateSchema = "ARRAY<STRUCT<SEQ BIGINT, VAL STRING>>")
  public static Udaf<String, List<Struct>, List<String>> earliestStrings(
      final int earliestN,
      final boolean ignoreNulls
  ) {
    return earliestN(STRUCT_STRING, earliestN, ignoreNulls);
  }

  @VisibleForTesting
  static <T> Struct createStruct(final Schema schema, final T val) {
    return KudafByOffsetUtils.createStruct(schema, generateSequence(), val);
  }

  private static long generateSequence() {
    return sequence.getAndIncrement();
  }

  @VisibleForTesting
  static <T> Udaf<T, Struct, T> earliest(
      final Schema structSchema,
      final boolean ignoreNulls
  ) {
    return new Udaf<T, Struct, T>() {

      @Override
      public Struct initialize() {
        return null;
      }

      @Override
      public Struct aggregate(final T current, final Struct aggregate) {
        if (aggregate != null) {
          return aggregate;
        }

        if (current == null && ignoreNulls) {
          return null;
        }

        return createStruct(structSchema, current);
      }

      @Override
      public Struct merge(final Struct aggOne, final Struct aggTwo) {
        if (aggOne == null) {
          return aggTwo;
        }

        if (aggTwo == null) {
          return aggOne;
        }

        // When merging we need some way of evaluating the "earliest' one.
        // We do this by keeping track of the sequence of when it was originally processed
        if (INTERMEDIATE_STRUCT_COMPARATOR.compare(aggOne, aggTwo) < 0) {
          return aggOne;
        } else {
          return aggTwo;
        }
      }

      @Override
      @SuppressWarnings("unchecked")
      public T map(final Struct agg) {
        if (agg == null) {
          return null;
        }

        return (T) agg.get(VAL_FIELD);
      }
    };
  }

  @VisibleForTesting
  static <T> Udaf<T, List<Struct>, List<T>> earliestN(
      final Schema structSchema,
      final int earliestN,
      final boolean ignoreNulls
  ) {
    if (earliestN <= 0) {
      throw new KsqlFunctionException("earliestN must be 1 or greater");
    }

    return new Udaf<T, List<Struct>, List<T>>() {
      @Override
      public List<Struct> initialize() {
        return new ArrayList<>(earliestN);
      }

      @Override
      public List<Struct> aggregate(final T current, final List<Struct> aggregate) {
        if (current == null && ignoreNulls) {
          return aggregate;
        }

        if (aggregate.size() < earliestN) {
          aggregate.add(createStruct(structSchema, current));
        }
        return aggregate;
      }

      @Override
      public List<Struct> merge(final List<Struct> aggOne, final List<Struct> aggTwo) {
        final List<Struct> merged = new ArrayList<>(aggOne.size() + aggTwo.size());
        merged.addAll(aggOne);
        merged.addAll(aggTwo);
        merged.sort(INTERMEDIATE_STRUCT_COMPARATOR);
        return merged.subList(0, Math.min(earliestN, merged.size()));
      }

      @Override
      @SuppressWarnings("unchecked")
      public List<T> map(final List<Struct> agg) {
        return (List<T>) agg.stream().map(s -> s.get(VAL_FIELD)).collect(Collectors.toList());
      }
    };
  }
}
