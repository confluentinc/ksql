/*
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

package io.confluent.ksql.function.udaf.topk;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.util.KsqlException;

public class TopKAggregateFunctionFactory extends AggregateFunctionFactory {
  private static final String NAME = "TOPK";
  private final int topKSize;

  public TopKAggregateFunctionFactory() {
    super(NAME, createFunctions());
    this.topKSize = 0;
  }

  TopKAggregateFunctionFactory(final int topKSize) {
    super(NAME, Collections.emptyList());
    this.topKSize = topKSize;
  }

  // Just used to populate the function registry with info on the available functions
  private static List<KsqlAggregateFunction<?,?>> createFunctions() {
    return Arrays.asList(new TopkKudaf<>(
            NAME,
            -1,
            0,
            SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build(),
            Collections.singletonList(Schema.OPTIONAL_INT32_SCHEMA),
            Integer.class),
        new TopkKudaf<>(
            NAME,
            -1,
            0,
            SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build(),
            Collections.singletonList(Schema.OPTIONAL_INT64_SCHEMA),
            Long.class),
        new TopkKudaf<>(
            NAME,
            -1,
            0,
            SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build(),
            Collections.singletonList(Schema.OPTIONAL_FLOAT64_SCHEMA),
            Double.class),
        new TopkKudaf<>(
            NAME,
            -1,
            0,
            SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(),
            Collections.singletonList(Schema.OPTIONAL_STRING_SCHEMA),
            String.class));
  }

  @Override
  public KsqlAggregateFunction getProperAggregateFunction(final List<Schema> argumentType) {
    if (argumentType.isEmpty()) {
      throw new KsqlException("TOPK function should have two arguments.");
    }
    final Schema argSchema = argumentType.get(0);
    switch (argSchema.type()) {
      case INT32:
        return new TopkKudaf<>(
            NAME,
            -1,
            topKSize,
            SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build(),
            Collections.singletonList(Schema.OPTIONAL_INT32_SCHEMA),
            Integer.class);
      case INT64:
        return new TopkKudaf<>(
            NAME,
            -1,
            topKSize,
            SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build(),
            Collections.singletonList(Schema.OPTIONAL_INT64_SCHEMA),
            Long.class);
      case FLOAT64:
        return new TopkKudaf<>(
            NAME,
            -1,
            topKSize,
            SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build(),
            Collections.singletonList(Schema.OPTIONAL_FLOAT64_SCHEMA),
            Double.class);
      case STRING:
        return new TopkKudaf<>(
            NAME,
            -1,
            topKSize,
            SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(),
            Collections.singletonList(Schema.OPTIONAL_STRING_SCHEMA),
            String.class);
      default:
        throw new KsqlException("No TOPK aggregate function with " + argumentType.get(0)
                                + " argument type exists!");
    }
  }
}
