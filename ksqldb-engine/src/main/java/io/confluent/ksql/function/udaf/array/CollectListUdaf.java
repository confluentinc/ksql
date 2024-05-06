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

package io.confluent.ksql.function.udaf.array;

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConstants;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.Configurable;

@UdafDescription(
    name = "collect_list",
    description = "Gather all of the values from an input grouping into a single Array field."
        + "\nAlthough this aggregate works on both Stream and Table inputs, the order of entries"
        + " in the result array is not guaranteed when working on Table input data."
        + "\nYou may limit the size of the resultant Array to N entries, beyond which"
        + " any further values will be silently ignored, by setting the"
        + " ksql.functions.collect_list.limit configuration to N.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public final class CollectListUdaf {

  public static final String LIMIT_CONFIG = "ksql.functions.collect_list.limit";

  private CollectListUdaf() {
    // just to make the checkstyle happy
  }

  @UdafFactory(description = "collect values of a field into a single Array")
  public static <T> TableUdaf<T, List<T>, List<T>> createCollectListT() {
    return new Collect<>();
  }

  private static final class Collect<T> implements TableUdaf<T, List<T>, List<T>>, Configurable {

    private int limit = Integer.MAX_VALUE;
    SqlType inputType;

    @Override
    public void configure(final Map<String, ?> map) {
      final Object limit = map.get(LIMIT_CONFIG);
      if (limit != null) {
        if (limit instanceof Number) {
          this.limit = ((Number) limit).intValue();
        } else if (limit instanceof String) {
          this.limit = Integer.parseInt((String) limit);
        }
      }

      if (this.limit < 0) {
        this.limit = Integer.MAX_VALUE;
      }
    }

    @Override
    public void initializeTypeArguments(final List<SqlArgument> argTypeList) {
      inputType = argTypeList.get(0).getSqlTypeOrThrow();
    }

    @Override
    public Optional<SqlType> getAggregateSqlType() {
      return Optional.of(SqlArray.of(inputType));
    }

    @Override
    public Optional<SqlType> getReturnSqlType() {
      return Optional.of(SqlArray.of(inputType));
    }

    @Override
    public List<T> initialize() {
      return new ArrayList<>();
    }

    @Override
    public List<T> aggregate(final T thisValue, final List<T> aggregate) {
      if (aggregate.size() < limit) {
        aggregate.add(thisValue);
      }
      return aggregate;
    }

    @Override
    public List<T> merge(final List<T> aggOne, final List<T> aggTwo) {
      final int remainingCapacity = limit - aggOne.size();
      aggOne.addAll(aggTwo.subList(0, Math.min(remainingCapacity, aggTwo.size())));
      return aggOne;
    }

    @Override
    public List<T> map(final List<T> agg) {
      return agg;
    }

    @Override
    public List<T> undo(final T valueToUndo, final List<T> aggregateValue) {
      // A more ideal solution would remove the value which corresponded to the original insertion
      // but keeping track of that is more complex, so we just remove the last value for now.
      final int lastIndex = aggregateValue.lastIndexOf(valueToUndo);
      // If we cannot find the value, that means that we hit the limit and never inserted it, so
      // just return.
      if (lastIndex < 0) {
        return aggregateValue;
      }
      aggregateValue.remove(lastIndex);
      return aggregateValue;
    }
  }
}
