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
import java.util.List;
import java.util.Optional;
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

  static final AtomicLong sequence = new AtomicLong();

  @UdafFactory(description = "return the earliest value of a column")
  public static <T> Udaf<T, Struct, T> earliest() {
    return earliest(true);
  }

  @UdafFactory(description = "return the earliest value of a column")
  public static <T> Udaf<T, Struct, T> earliest(final boolean ignoreNulls) {
    return earliestT(ignoreNulls);
  }

  @UdafFactory(description = "return the earliest N values of a column")
  public static <T> Udaf<T, List<Struct>, List<T>> earliest(final int earliestN) {
    return earliest(earliestN, true);
  }

  @UdafFactory(description = "return the earliest N values of a column")
  public static <T> Udaf<T, List<Struct>, List<T>> earliest(final int earliestN,
      final boolean ignoreNulls) {
    return earliestTN(earliestN, ignoreNulls);
  }

  @VisibleForTesting
  static <T> Struct createStruct(final Schema schema, final T val) {
    return KudafByOffsetUtils.createStruct(schema, generateSequence(), val);
  }

  private static long generateSequence() {
    return sequence.getAndIncrement();
  }

  @VisibleForTesting
  static <T> Udaf<T, Struct, T> earliestT(
      final boolean ignoreNulls
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

        // When merging we need some way of evaluating the "earliest" one.
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
  static <T> Udaf<T, List<Struct>, List<T>> earliestTN(
      final int earliestN,
      final boolean ignoreNulls
  ) {
    if (earliestN <= 0) {
      throw new KsqlFunctionException("earliestN must be 1 or greater");
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
