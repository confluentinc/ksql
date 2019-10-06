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

package io.confluent.ksql.function.udaf.count;

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;

@UdafDescription(name = "count",
    description = "Counts the number of rows. When a column argument is specified, the count"
        + " returned will be the number of rows where col1 is non-null. When * is specified,"
        + " the count returned will be the total number of rows."
)
public final class CountUdaf {

  private CountUdaf() {
  }

  @UdafFactory(description = "Count the number of rows.")
  public static <I> TableUdaf<I, Long, Long> count() {

    return new TableUdaf<I, Long, Long>() {

      @Override
      public Long initialize() {
        return 0L;
      }

      @Override
      public Long aggregate(final Object newValue,
                            final Long aggregateValue) {

        if (newValue == null) {
          return aggregateValue;
        }
        return aggregateValue + 1;
      }

      @Override
      public Long map(final Long aggregateValue) {
        return aggregateValue;
      }

      @Override
      public Long merge(final Long agg1,
                        final Long agg2) {

        return agg1 + agg2;
      }

      @Override
      public Long undo(final Object valueToUndo,
                       final Long aggregateValue) {
        if (valueToUndo == null) {
          return aggregateValue;
        }
        return aggregateValue - 1;
      }
    };
  }
}