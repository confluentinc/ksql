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

package io.confluent.ksql.function.udaf.topk;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.AggregateFunctionInitArguments;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public class TopKAggregateFunctionFactory extends AggregateFunctionFactory {

  private static final String NAME = "TOPK";

  private static final List<List<Schema>> SUPPORTED_TYPES = ImmutableList
      .<List<Schema>>builder()
      .add(ImmutableList.of(Schema.OPTIONAL_INT32_SCHEMA))
      .add(ImmutableList.of(Schema.OPTIONAL_INT64_SCHEMA))
      .add(ImmutableList.of(Schema.OPTIONAL_FLOAT64_SCHEMA))
      .add(ImmutableList.of(Schema.OPTIONAL_STRING_SCHEMA))
      .build();

  public TopKAggregateFunctionFactory() {
    super(NAME);
  }

  private static final AggregateFunctionInitArguments DEFAULT_INIT_ARGS =
      new AggregateFunctionInitArguments(0, "1");

  @Override
  public KsqlAggregateFunction createAggregateFunction(
      final List<Schema> argumentType,
      final AggregateFunctionInitArguments initArgs
  ) {
    if (argumentType.isEmpty()) {
      throw new KsqlException("TOPK function should have two arguments.");
    }
    final int tkValFromArg = Integer.parseInt(initArgs.arg(0));
    final Schema argSchema = argumentType.get(0);
    switch (argSchema.type()) {
      case INT32:
        return new TopkKudaf<>(
            NAME,
            initArgs.udafIndex(),
            tkValFromArg,
            SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build(),
            Collections.singletonList(Schema.OPTIONAL_INT32_SCHEMA),
            Integer.class);
      case INT64:
        return new TopkKudaf<>(
            NAME,
            initArgs.udafIndex(),
            tkValFromArg,
            SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build(),
            Collections.singletonList(Schema.OPTIONAL_INT64_SCHEMA),
            Long.class);
      case FLOAT64:
        return new TopkKudaf<>(
            NAME,
            initArgs.udafIndex(),
            tkValFromArg,
            SchemaBuilder.array(Schema.OPTIONAL_FLOAT64_SCHEMA).optional().build(),
            Collections.singletonList(Schema.OPTIONAL_FLOAT64_SCHEMA),
            Double.class);
      case STRING:
        return new TopkKudaf<>(
            NAME,
            initArgs.udafIndex(),
            tkValFromArg,
            SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build(),
            Collections.singletonList(Schema.OPTIONAL_STRING_SCHEMA),
            String.class);
      default:
        throw new KsqlException("No TOPK aggregate function with " + argumentType.get(0)
            + " argument type exists!");
    }
  }

  @Override
  public List<List<Schema>> supportedArgs() {
    return SUPPORTED_TYPES;
  }

  @Override
  public AggregateFunctionInitArguments getDefaultArguments() {
    return DEFAULT_INIT_ARGS;
  }
}
