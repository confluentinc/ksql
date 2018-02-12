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

package io.confluent.ksql.function.udaf;

import io.confluent.ksql.GenericRow;
import org.apache.kafka.streams.kstream.Initializer;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KudafInitializer implements Initializer<GenericRow> {

  private final List<Supplier> aggValueSuppliers = new ArrayList<>();
  private final int nonAggValSize;

  public KudafInitializer(final int nonAggValSize) {
    this.nonAggValSize = nonAggValSize;
  }

  @Override
  public GenericRow apply() {
    final List<Object> values = IntStream.range(0, nonAggValSize)
        .mapToObj(value -> null)
        .collect(Collectors.toList());

    aggValueSuppliers.forEach(supplier -> values.add(supplier.get()));
    return new GenericRow(values);
  }

  public void addAggregateIntializer(Supplier intialValueSupplier) {
    aggValueSuppliers.add(intialValueSupplier);
  }
}
