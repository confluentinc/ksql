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

public class DoubleSumKudaf extends SumKudaf<Double> {

  @Override
  public Double initialize() {
    return 0.0;
  }

  @Override
  public Double aggregate(final Double valueToAdd, final Double aggregateValue) {
    if (valueToAdd == null) {
      return aggregateValue;
    }
    return aggregateValue + valueToAdd;
  }

  @Override
  public Double merge(final Double aggOne, final Double aggTwo) {
    return aggOne + aggTwo;
  }

  @Override
  public Double map(final Double agg) {
    return agg;
  }

  @Override
  public Double undo(final Double valueToUndo, final Double aggregateValue) {
    if (valueToUndo == null) {
      return aggregateValue;
    }
    return aggregateValue - valueToUndo;
  }
  
}
