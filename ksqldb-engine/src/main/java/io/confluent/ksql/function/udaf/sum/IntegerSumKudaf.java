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

public class IntegerSumKudaf extends SumKudaf<Integer> {

  @Override
  public Integer initialize() {
    return 0;
  }

  @Override
  public Integer aggregate(final Integer valueToAdd, final Integer aggregateValue) {
    if (valueToAdd == null) {
      return aggregateValue;
    }
    return aggregateValue + valueToAdd;
  }

  @Override
  public Integer merge(final Integer aggOne, final Integer aggTwo) {
    return aggOne + aggTwo;
  }

  @Override
  public Integer map(final Integer agg) {
    return agg;
  }

  @Override
  public Integer undo(final Integer valueToUndo, final Integer aggregateValue) {
    if (valueToUndo == null) {
      return aggregateValue;
    }
    return aggregateValue - valueToUndo;
  }

}