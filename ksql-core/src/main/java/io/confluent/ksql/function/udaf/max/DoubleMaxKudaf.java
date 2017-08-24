/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.function.udaf.max;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Merger;

import java.util.Arrays;

import io.confluent.ksql.function.KsqlAggregateFunction;

public class DoubleMaxKudaf extends KsqlAggregateFunction<Double, Double> {

  public DoubleMaxKudaf(Integer argIndexInValue) {
    super(argIndexInValue, Double.MIN_VALUE, Schema.FLOAT64_SCHEMA,
          Arrays.asList(Schema.FLOAT64_SCHEMA),
          "MAX", DoubleMaxKudaf.class);
  }

  @Override
  public Double aggregate(Double currentVal, Double currentAggVal) {
    if (currentVal > currentAggVal) {
      return currentVal;
    }
    return currentAggVal;
  }

  @Override
  public Merger getMerger() {
    return new Merger<String, Double>() {
      @Override
      public Double apply(final String aggKey, final Double aggOne, final Double aggTwo) {
        if (aggOne > aggTwo) {
          return aggOne;
        }
        return aggTwo;
      }
    };
  }
}
