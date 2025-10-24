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

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConstants;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.Configurable;

@UdafDescription(
    name = "collect_map",
    description = "Gather all of the values from an input grouping into a single Map field."
        + "\nNot available for aggregating values from an input Table."
        + "\nYou may limit the size of the resultant Map to N entries, beyond which"
        + " any further values will be silently ignored, by setting the"
        + " ksql.functions.collect_map.limit configuration to N.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public final class CollectMapUdaf {

  public static final String LIMIT_CONFIG = "ksql.functions.collect_map.limit";

  private CollectMapUdaf() {
    // just to make the checkstyle happy
  }

  @UdafFactory(description = "collect Maps into a single Map")
  public static <K,V> Udaf<Map<K,V>, Map<K,V>, Map<K,V>> createCollecMapT() {
    return new CollectMap<>();
  }

  private static final class CollectMap<K,V> implements Udaf<Map<K,V>, Map<K,V>, Map<K,V>>,
      Configurable {

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
      return Optional.of(inputType);
    }

    @Override
    public Optional<SqlType> getReturnSqlType() {
      return Optional.of(inputType);
    }

    @Override
    public Map<K, V> initialize() {
      return new HashMap<>();
    }

    @Override
    public Map<K, V> aggregate(final Map<K, V> current, final Map<K, V> aggregate) {
      aggregate.putAll(current);
      return aggregate;
    }

    @Override
    public Map<K, V> merge(final Map<K, V> aggOne, final Map<K, V> aggTwo) {
      // JNH: Think about this order.
      aggOne.putAll(aggTwo);
      return null;
    }

    @Override
    public Map<K, V> map(final Map<K, V> agg) {
      return agg;
    }
  }
}
