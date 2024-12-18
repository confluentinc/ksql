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

package io.confluent.ksql.function.udaf.count;

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.util.KsqlConstants;

@UdafDescription(
        name = "COUNT",
        description = CountKudaf.DESCRIPTION,
        author = KsqlConstants.CONFLUENT_AUTHOR
)
public class CountKudaf<T> implements TableUdaf<T, Long, Long> {
  public static final String DESCRIPTION = "Counts records by key.";

  @UdafFactory(description = CountKudaf.DESCRIPTION)
  public static <T> TableUdaf<T, Long, Long> createCount() {
    return new CountKudaf<>();
  }

  @Override
  public Long aggregate(final Object currentValue, final Long aggregateValue) {
    if (currentValue == null) {
      return aggregateValue;
    }
    return aggregateValue + 1;
  }

  @Override
  public Long initialize() {
    return 0L;
  }

  @Override
  public Long merge(final Long aggOne, final Long aggTwo) {
    return aggOne + aggTwo;
  }

  @Override
  public Long map(final Long agg) {
    return agg;
  }

  @Override
  public Long undo(final T valueToUndo, final Long aggregateValue) {
    if (valueToUndo == null) {
      return aggregateValue;
    }
    return aggregateValue - 1;
  }
}
