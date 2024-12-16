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
import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.INTERMEDIATE_STRUCT_COMPARATOR_IGNORE_NULLS;
import static io.confluent.ksql.function.udaf.offset.KudafByOffsetUtils.VAL_FIELD;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConstants;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
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
      "This function returns the oldest N values for the column, computed by offset.";

  private LatestByOffset() {
  }

  static final AtomicLong sequence = new AtomicLong();

  @UdafFactory(description = "return the latest value of a column")
  public static <T> Udaf<T, Struct, T> latest() {
    return latest(true);
  }

  @UdafFactory(description = "return the latest value of a column")
  public static <T> Udaf<T, Struct, T> latest(final boolean ignoreNulls) {
    return latestT(ignoreNulls, getComparator(ignoreNulls));
  }

  @UdafFactory(description = "return the latest N values of a column")
  public static <T> Udaf<T, List<Struct>, List<T>> latest(final int latestN) {
    return latest(latestN, true);
  }

  @UdafFactory(description = "return the latest N values of a column")
  public static <T> Udaf<T, List<Struct>, List<T>> latest(final int latestN,
      final boolean ignoreNulls) {
    return latestTN(latestN, ignoreNulls, getComparator(ignoreNulls));
  }

  @VisibleForTesting
  static <T> Struct createStruct(final Schema schema, final T val) {
    return KudafByOffsetUtils.createStruct(schema, generateSequence(), val);
  }

  private static long generateSequence() {
    return sequence.getAndIncrement();
  }

  @VisibleForTesting
  static <T> Udaf<T, Struct, T> latestT(
      final boolean ignoreNulls,
      final Comparator<Struct> comparator
  ) {
    return new Udaf<T, Struct, T>() {
      Schema structSchema;
      SqlType aggregateType;
      SqlType returnType;

      @Override
      public void initializeTypeArguments(final List<SqlArgument> argTypeList) {
        returnType = argTypeList.get(0).getSqlTypeOrThrow();
        final Schema connectType =
            SchemaConverters.sqlToConnectConverter().toConnectSchema(returnType);
        structSchema = KudafByOffsetUtils.buildSchema(connectType);
        aggregateType = SchemaConverters.connectToSqlConverter().toSqlType(structSchema);
      }

      @Override
      public Optional<SqlType> getAggregateSqlType() {
        return Optional.of(aggregateType);
      }

      @Override
      public Optional<SqlType> getReturnSqlType() {
        return Optional.of(returnType);
      }

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
        // When merging we need some way of evaluating the "latest" one.
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
  static <T> Udaf<T, List<Struct>, List<T>> latestTN(
      final int latestN,
      final boolean ignoreNulls,
      final Comparator<Struct> comparator
  ) {
    if (latestN <= 0) {
      throw new KsqlFunctionException("latestN must be 1 or greater");
    }

    return new Udaf<T, List<Struct>, List<T>>() {
      Schema structSchema;
      SqlType aggregateType;
      SqlType returnType;

      @Override
      public void initializeTypeArguments(final List<SqlArgument> argTypeList) {
        final SqlType inputType = argTypeList.get(0).getSqlTypeOrThrow();
        final Schema connectType =
            SchemaConverters.sqlToConnectConverter().toConnectSchema(inputType);
        structSchema = KudafByOffsetUtils.buildSchema(connectType);
        aggregateType =
            SqlArray.of(SchemaConverters.connectToSqlConverter().toSqlType(structSchema));
        returnType = SqlArray.of(inputType);
      }

      @Override
      public Optional<SqlType> getAggregateSqlType() {
        return Optional.of(aggregateType);
      }

      @Override
      public Optional<SqlType> getReturnSqlType() {
        return Optional.of(returnType);
      }

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
        merged.sort(INTERMEDIATE_STRUCT_COMPARATOR);
        return merged.subList(0, Math.min(latestN, merged.size()));
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
