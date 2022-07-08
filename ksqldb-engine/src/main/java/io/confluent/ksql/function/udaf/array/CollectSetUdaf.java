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

import com.google.common.collect.Lists;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.util.KsqlConstants;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.Configurable;

@UdafDescription(name = "collect_set", 
    description = "Gather all of the distinct values from an input grouping into a single Array."
        + "\nNot available for aggregating values from an input Table."
        + "\nYou may limit the size of the resultant Array to N entries, beyond which"
        + " any further values will be silently ignored, by setting the"
        + " ksql.functions.collect_list.limit configuration to N.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public final class CollectSetUdaf {

  public static final String LIMIT_CONFIG = "ksql.functions.collect_set.limit";

  private CollectSetUdaf() {
    // just to make the checkstyle happy
  }
  
  @UdafFactory(description = "collect distinct values of a Bigint field into a single Array")
  public static Udaf<Long, List<Long>, List<Long>> createCollectSetLong() {
    return new Collect<>();
  }

  @UdafFactory(description = "collect distinct values of an Integer field into a single Array")
  public static Udaf<Integer, List<Integer>, List<Integer>> createCollectSetInt() {
    return new Collect<>();
  }

  @UdafFactory(description = "collect distinct values of a Double field into a single Array")
  public static Udaf<Double, List<Double>, List<Double>> createCollectSetDouble() {
    return new Collect<>();
  }

  @UdafFactory(description = "collect distinct values of a String field into a single Array")
  public static Udaf<String, List<String>, List<String>> createCollectSetString() {
    return new Collect<>();
  }

  @UdafFactory(description = "collect distinct values of a Boolean field into a single Array")
  public static Udaf<Boolean, List<Boolean>, List<Boolean>> createCollectSetBool() {
    return new Collect<>();
  }

  @UdafFactory(description = "collect distinct values of a Timestamp field into a single Array")
  public static Udaf<Timestamp, List<Timestamp>, List<Timestamp>> createCollectSetTimestamp() {
    return new Collect<>();
  }

  @UdafFactory(description = "collect distinct values of a Time field into a single Array")
  public static Udaf<Time, List<Time>, List<Time>> createCollectSetTime() {
    return new Collect<>();
  }

  @UdafFactory(description = "collect distinct values of a Date field into a single Array")
  public static Udaf<Date, List<Date>, List<Date>> createCollectSetDate() {
    return new Collect<>();
  }

  @UdafFactory(description = "collect distinct values of a Bytes field into a single Array")
  public static Udaf<ByteBuffer, List<ByteBuffer>, List<ByteBuffer>> createCollectSetBytes() {
    return new Collect<>();
  }

  private static final class Collect<T> implements Udaf<T, List<T>, List<T>>, Configurable {

    private int limit = Integer.MAX_VALUE;

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
    public List<T> initialize() {
      return Lists.newArrayList();
    }

    @Override
    public List<T> aggregate(final T thisValue, final List<T> aggregate) {
      if (aggregate.size() < limit && !aggregate.contains(thisValue)) {
        aggregate.add(thisValue);
      }
      return aggregate;
    }

    @Override
    public List<T> merge(final List<T> aggOne, final List<T> aggTwo) {
      for (final T thisEntry : aggTwo) {
        if (aggOne.size() == limit) {
          break;
        }
        if (!aggOne.contains(thisEntry)) {
          aggOne.add(thisEntry);
        }
      }
      return aggOne;
    }

    @Override
    public List<T> map(final List<T> agg) {
      return agg;
    }
  }

}
