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

package io.confluent.ksql.function.udaf.sum;

import io.confluent.ksql.function.KsqlAggregateFunction;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Merger;

import java.util.Arrays;

public class DoubleSumKudaf extends KsqlAggregateFunction<Double, Double> {

  public DoubleSumKudaf(Integer argIndexInValue) {
    super(argIndexInValue, 0.0, Schema.FLOAT64_SCHEMA,
          Arrays.asList(Schema.FLOAT64_SCHEMA),
          "SUM", DoubleSumKudaf.class);
  }

  @Override
  public Double aggregate(Double currentVal, Double currentAggVal) {
    return currentVal + currentAggVal;
  }

  @Override
  public Merger<String, Double> getMerger() {
    return (aggKey, aggOne, aggTwo) -> aggOne + aggTwo;
  }


}
