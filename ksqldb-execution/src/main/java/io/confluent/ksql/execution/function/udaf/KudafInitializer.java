/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.function.udaf;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.kafka.streams.kstream.Initializer;

public class KudafInitializer implements Initializer<GenericRow> {

  private final List<Supplier<?>> initialValueSuppliers;
  private final int nonAggValSize;

  public KudafInitializer(final int nonAggValSize, final List<Supplier<?>> initialValueSuppliers) {
    this.nonAggValSize = nonAggValSize;
    this.initialValueSuppliers = ImmutableList.copyOf(
        Objects.requireNonNull(initialValueSuppliers, "initialValueSuppliers")
    );
  }

  @Override
  public GenericRow apply() {
    final GenericRow row = new GenericRow(nonAggValSize + initialValueSuppliers.size());

    IntStream.range(0, nonAggValSize)
        .forEach(i -> row.append(null));

    initialValueSuppliers.forEach(supplier -> row.append(supplier.get()));
    return row;
  }
}
