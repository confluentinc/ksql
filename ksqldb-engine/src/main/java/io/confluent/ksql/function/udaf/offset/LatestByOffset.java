/*
 * Copyright 2019 Confluent Inc.
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
import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.INTERMEDIATE_STRUCT_COMPARATOR_IGNORE_NULLS;
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
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(
    name = "LATEST_BY_OFFSET",
    description = LatestByOffset.DESCRIPTION,
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public final class LatestByOffset {

  static final String DESCRIPTION =
      "This function returns the most recent N values for the column, computed by offset.";

  private LatestByOffset() {
  }

  static AtomicLong sequence = new AtomicLong();

  @UdafFactory(description = "return the latest value of an integer column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL INT>")
  public static Udaf<Integer, Struct, Integer> latestInteger() {
    return latestInteger(true);
  }

  @UdafFactory(description = "return the latest value of an integer column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL INT>")
  public static Udaf<Integer, Struct, Integer> latestInteger(final boolean ignoreNulls) {
    return latest(STRUCT_INTEGER, ignoreNulls, getComparator(ignoreNulls));
  }

  @UdafFactory(description = "return the latest N value of an integer column",
      aggregateSchema = "ARRAY<STRUCT<SEQ BIGINT, VAL INT>>")
  public static Udaf<Integer, List<Struct>, List<Integer>> latestIntegers(final int latestN) {
    return latestIntegers(latestN, true);
  }

  @UdafFactory(description = "return the latest N value of an integer column",
      aggregateSchema = "ARRAY<STRUCT<SEQ BIGINT, VAL INT>>")
  public static Udaf<Integer, List<Struct>, List<Integer>> latestIntegers(
      final int latestN,
      final boolean ignoreNulls
  ) {
    return latestN(STRUCT_INTEGER, latestN, ignoreNulls, getComparator(ignoreNulls));
  }

  @UdafFactory(description = "return the latest value of an big integer column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL BIGINT>")
  public static Udaf<Long, Struct, Long> latestLong() {
    return latestLong(true);
  }

  @UdafFactory(description = "return the latest value of an big integer column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL BIGINT>")
  public static Udaf<Long, Struct, Long> latestLong(final boolean ignoreNulls) {
    return latest(STRUCT_LONG, ignoreNulls, getComparator(ignoreNulls));
  }

  @UdafFactory(description = "return the latest N value of an big integer column",
      aggregateSchema = "ARRAY<STRUCT<SEQ BIGINT, VAL BIGINT>>")
  public static Udaf<Long, List<Struct>, List<Long>> latestLongs(final int latestN) {
    return latestLongs(latestN, true);
  }

  @UdafFactory(description = "return the latest N value of an big integer column",
      aggregateSchema = "ARRAY<STRUCT<SEQ BIGINT, VAL BIGINT>>")
  public static Udaf<Long, List<Struct>, List<Long>> latestLongs(
      final int latestN,
      final boolean ignoreNulls
  ) {
    return latestN(STRUCT_LONG, latestN, ignoreNulls, getComparator(ignoreNulls));
  }

  @UdafFactory(description = "return the latest value of a double column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL DOUBLE>")
  public static Udaf<Double, Struct, Double> latestDouble() {
    return latestDouble(true);
  }

  @UdafFactory(description = "return the latest value of a double column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL DOUBLE>")
  public static Udaf<Double, Struct, Double> latestDouble(final boolean ignoreNulls) {
    return latest(STRUCT_DOUBLE, ignoreNulls, getComparator(ignoreNulls));
  }

  @UdafFactory(description = "return the latest N values of a double column",
      aggregateSchema = "ARRAY<STRUCT<SEQ BIGINT, VAL DOUBLE>>")
  public static Udaf<Double, List<Struct>, List<Double>> latestDoubles(final int latestN) {
    return latestDoubles(latestN, true);
  }

  @UdafFactory(description = "return the latest N values of a double column",
      aggregateSchema = "ARRAY<STRUCT<SEQ BIGINT, VAL DOUBLE>>")
  public static Udaf<Double, List<Struct>, List<Double>> latestDoubles(
      final int latestN,
      final boolean ignoreNulls
  ) {
    return latestN(STRUCT_DOUBLE, latestN, ignoreNulls, getComparator(ignoreNulls));
  }

  @UdafFactory(description = "return the latest value of a boolean column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL BOOLEAN>")
  public static Udaf<Boolean, Struct, Boolean> latestBoolean() {
    return latestBoolean(true);
  }

  @UdafFactory(description = "return the latest value of a boolean column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL BOOLEAN>")
  public static Udaf<Boolean, Struct, Boolean> latestBoolean(final boolean ignoreNulls) {
    return latest(STRUCT_BOOLEAN, ignoreNulls, getComparator(ignoreNulls));
  }

  @UdafFactory(description = "return the latest N value of a boolean column",
      aggregateSchema = "ARRAY<STRUCT<SEQ BIGINT, VAL BOOLEAN>>")
  public static Udaf<Boolean, List<Struct>, List<Boolean>> latestBooleans(final int latestN) {
    return latestBooleans(latestN, true);
  }

  @UdafFactory(description = "return the latest N value of a boolean column",
      aggregateSchema = "ARRAY<STRUCT<SEQ BIGINT, VAL BOOLEAN>>")
  public static Udaf<Boolean, List<Struct>, List<Boolean>> latestBooleans(
      final int latestN,
      final boolean ignoreNulls
  ) {
    return latestN(STRUCT_BOOLEAN, latestN, ignoreNulls, getComparator(ignoreNulls));
  }

  @UdafFactory(description = "return the latest value of a string column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL STRING>")
  public static Udaf<String, Struct, String> latestString() {
    return latestString(true);
  }

  @UdafFactory(description = "return the latest value of a string column",
      aggregateSchema = "STRUCT<SEQ BIGINT, VAL STRING>")
  public static Udaf<String, Struct, String> latestString(final boolean ignoreNulls) {
    return latest(STRUCT_STRING, ignoreNulls, getComparator(ignoreNulls));
  }

  @UdafFactory(description = "return the latest N value of a string column",
      aggregateSchema = "ARRAY<STRUCT<SEQ BIGINT, VAL STRING>>")
  public static Udaf<String, List<Struct>, List<String>> latestStrings(final int latestN) {
    return latestStrings(latestN, true);
  }

  @UdafFactory(description = "return the latest N value of a string column",
      aggregateSchema = "ARRAY<STRUCT<SEQ BIGINT, VAL STRING>>")
  public static Udaf<String, List<Struct>, List<String>> latestStrings(
      final int latestN,
      final boolean ignoreNulls
  ) {
    return latestN(STRUCT_STRING, latestN, ignoreNulls, getComparator(ignoreNulls));
  }

  @VisibleForTesting
  static <T> Struct createStruct(final Schema schema, final T val) {
    return KudafByOffsetUtils.createStruct(schema, generateSequence(), val);
  }

  private static long generateSequence() {
    return sequence.getAndIncrement();
  }

  @VisibleForTesting
  static <T> Udaf<T, Struct, T> latest(
      final Schema structSchema,
      final boolean ignoreNulls,
      final Comparator<Struct> comparator
  ) {
    return new Udaf<T, Struct, T>() {

      @Override
      public Struct initialize() {
        return createStruct(structSchema, null);
      }

      @Override
      public Struct aggregate(final T current, final Struct aggregate) {
        if (current == null && ignoreNulls) {
          return aggregate;
        }

        return createStruct(structSchema, current);
      }

      @Override
      public Struct merge(final Struct aggOne, final Struct aggTwo) {
        // When merging we need some way of evaluating the "latest' one.
        // We do this by keeping track of the sequence of when it was originally processed
        if (comparator.compare(aggOne, aggTwo) >= 0) {
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

  @VisibleForTesting
  static <T> Udaf<T, List<Struct>, List<T>> latestN(
      final Schema structSchema,
      final int latestN,
      final boolean ignoreNulls,
      final Comparator<Struct> comparator
  ) {

    if (latestN <= 0) {
      throw new KsqlFunctionException("earliestN must be 1 or greater");
    }

    return new Udaf<T, List<Struct>, List<T>>() {

      @Override
      public List<Struct> initialize() {
        return new ArrayList<>(latestN);
      }

      @Override
      public List<Struct> aggregate(final T current, final List<Struct> aggregate) {
        if (current == null && ignoreNulls) {
          return aggregate;
        }

        aggregate.add(createStruct(structSchema, current));
        final int currentSize = aggregate.size();
        if (currentSize > latestN) {
          return aggregate.subList(currentSize - latestN, currentSize);
        }
        return aggregate;
      }

      @Override
      public List<Struct> merge(final List<Struct> aggOne, final List<Struct> aggTwo) {
        final List<Struct> merged = new ArrayList<>(aggOne.size() + aggTwo.size());
        merged.addAll(aggOne);
        merged.addAll(aggTwo);
        merged.sort(comparator);
        final int start = merged.size() > latestN ? (merged.size() - latestN) : 0;
        return merged.subList(start, merged.size());
      }

      @Override
      @SuppressWarnings("unchecked")
      public List<T> map(final List<Struct> agg) {
        return (List<T>) agg.stream().map(s -> s.get(VAL_FIELD)).collect(Collectors.toList());
      }
    };
  }

  private static Comparator<Struct> getComparator(final boolean ignoreNulls) {
    if (ignoreNulls) {
      return INTERMEDIATE_STRUCT_COMPARATOR_IGNORE_NULLS;
    }
    return INTERMEDIATE_STRUCT_COMPARATOR;
  }
}
