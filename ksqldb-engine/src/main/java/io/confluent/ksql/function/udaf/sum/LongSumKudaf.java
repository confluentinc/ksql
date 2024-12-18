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

package io.confluent.ksql.function.udaf.sum;

public class LongSumKudaf extends SumKudaf<Long> {

  @Override
  public Long initialize() {
    return 0L;
  }

  @Override
  public Long aggregate(final Long valueToAdd, final Long aggregateValue) {
    if (valueToAdd == null) {
      return aggregateValue;
    }
    return aggregateValue + valueToAdd;
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
  public Long undo(final Long valueToUndo, final Long aggregateValue) {
    if (valueToUndo == null) {
      return aggregateValue;
    }
    return aggregateValue - valueToUndo;
  }

}