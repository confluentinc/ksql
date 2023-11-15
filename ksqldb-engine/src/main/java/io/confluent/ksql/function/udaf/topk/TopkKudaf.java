/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.function.udaf.topk;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.function.udaf.VariadicArgs;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.Pair;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(
        name = "TOPK",
        description = "Returns the top k values for a column and other values in those records.",
        author = KsqlConstants.CONFLUENT_AUTHOR
)
public class TopkKudaf<T extends Comparable<? super T>, S>
        implements Udaf<Pair<T, VariadicArgs<Object>>, List<S>, List<S>> {

  @UdafFactory(description = "Returns the top k values for an integer column and other values in "
          + "those records.")
  public static <S> Udaf<Pair<Integer, VariadicArgs<Object>>, List<S>, List<S>>
      createTopKInt(final int k) {
    return new TopkKudaf<>(k);
  }

  @UdafFactory(description = "Returns the top k values for a bigint column and other values in "
          + "those records.")
  public static <S> Udaf<Pair<Long, VariadicArgs<Object>>, List<S>, List<S>>
      createTopKLong(final int k) {
    return new TopkKudaf<>(k);
  }

  @UdafFactory(description = "Returns the top k values for a double column and other values in "
          + "those records.")
  public static <S> Udaf<Pair<Double, VariadicArgs<Object>>, List<S>, List<S>>
      createTopKDouble(final int k) {
    return new TopkKudaf<>(k);
  }

  @UdafFactory(description = "Returns the top k values for a string column and other values in "
          + "those records.")
  public static <S> Udaf<Pair<String, VariadicArgs<Object>>, List<S>, List<S>>
      createTopKString(final int k) {
    return new TopkKudaf<>(k);
  }

  private static final String SORT_FIELD = "sort_col";
  private static final Function<Integer, String> OTHER_COL_TO_FIELD =
          (fieldNum) -> "col" + fieldNum;
  private final int topKSize;
  private Function<S, T> structToVal;
  private Function<Pair<T, VariadicArgs<Object>>, S> valToStruct;
  private SqlType aggregateSchema;

  TopkKudaf(final int topKSize) {
    this.topKSize = topKSize;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void initializeTypeArguments(final List<SqlArgument> argTypeList) {

    // We have to do some hacky casts here since we determine T and S from argTypeList
    if (argTypeList.size() > 2) {
      final Schema structSchema = makeStructSchema(argTypeList);
      aggregateSchema = SchemaConverters.connectToSqlConverter().toSqlType(structSchema);
      structToVal = (struct) -> (T) ((Struct) struct).get(SORT_FIELD);
      valToStruct = (pair) -> (S) makeStruct(structSchema, pair.getLeft(), pair.getRight());
    } else {
      aggregateSchema = argTypeList.get(0).getSqlTypeOrThrow();
      structToVal = (self) -> (T) self;
      valToStruct = (selfAndVarArgs) -> (S) selfAndVarArgs.getLeft();
    }

  }

  @Override
  public Optional<SqlType> getAggregateSqlType() {
    return Optional.of(SqlArray.of(aggregateSchema));
  }

  @Override
  public Optional<SqlType> getReturnSqlType() {
    return Optional.of(SqlArray.of(aggregateSchema));
  }

  @Override
  public List<S> initialize() {
    return new ArrayList<>();
  }

  @Override
  public List<S> aggregate(final Pair<T, VariadicArgs<Object>> currentValue,
                           final List<S> aggregateValue) {
    if (currentValue.getLeft() == null) {
      return aggregateValue;
    }

    final int currentSize = aggregateValue.size();
    if (!aggregateValue.isEmpty()) {
      final T last = structToVal.apply(aggregateValue.get(currentSize - 1));
      if (currentValue.getLeft().compareTo(last) <= 0
          && currentSize == topKSize) {
        return aggregateValue;
      }
    }

    if (currentSize == topKSize) {
      aggregateValue.set(currentSize - 1, valToStruct.apply(currentValue));
    } else {
      aggregateValue.add(valToStruct.apply(currentValue));
    }

    aggregateValue.sort(Comparator.comparing(structToVal).reversed());
    return aggregateValue;
  }

  @Override
  public List<S> merge(final List<S> aggOne, final List<S> aggTwo) {
    final List<S> merged = new ArrayList<>(
            Math.min(topKSize, aggOne.size() + aggTwo.size()));

    int idx1 = 0;
    int idx2 = 0;
    for (int i = 0; i != topKSize; ++i) {
      final S s1;
      final T v1;
      if (idx1 < aggOne.size()) {
        s1 = aggOne.get(idx1);
        v1 = structToVal.apply(s1);
      } else {
        s1 = null;
        v1 = null;
      }

      final S s2;
      final T v2;
      if (idx2 < aggTwo.size()) {
        s2 = aggTwo.get(idx2);
        v2 = structToVal.apply(s2);
      } else {
        s2 = null;
        v2 = null;
      }

      if (v1 != null && (v2 == null || v1.compareTo(v2) >= 0)) {
        merged.add(s1);
        idx1++;
      } else if (v2 != null && (v1 == null || v1.compareTo(v2) < 0)) {
        merged.add(s2);
        idx2++;
      } else {
        break;
      }
    }

    return merged;
  }

  @Override
  public List<S> map(final List<S> agg) {
    return agg;
  }

  private Struct makeStruct(final Schema structSchema, final T sortCol,
                            final VariadicArgs<Object> otherCols) {
    final Struct struct = new Struct(structSchema);
    struct.put(SORT_FIELD, sortCol);

    for (int argIndex = 0; argIndex < otherCols.size(); argIndex++) {
      struct.put(OTHER_COL_TO_FIELD.apply(argIndex), otherCols.get(argIndex));
    }

    return struct;
  }

  private Schema makeStructSchema(final List<SqlArgument> argTypeList) {
    final SchemaBuilder builder = SchemaBuilder.struct().optional();

    for (int argIndex = 0; argIndex < argTypeList.size() - 1; argIndex++) {
      final SqlType argSchema = argTypeList.get(argIndex).getSqlTypeOrThrow();
      builder.field(
              argIndex == 0 ? SORT_FIELD : OTHER_COL_TO_FIELD.apply(argIndex - 1),
              SchemaConverters.sqlToConnectConverter().toConnectSchema(argSchema)
      );
    }

    return builder.build();
  }

}