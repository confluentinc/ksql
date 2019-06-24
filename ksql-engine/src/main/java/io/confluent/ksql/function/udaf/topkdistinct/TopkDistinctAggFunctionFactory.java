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

package io.confluent.ksql.function.udaf.topkdistinct;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.List;
import org.apache.kafka.connect.data.Schema;

public class TopkDistinctAggFunctionFactory extends AggregateFunctionFactory {

  private static final String NAME = "TOPKDISTINCT";

  private static final List<List<Schema>> SUPPORTED_TYPES = ImmutableList
      .<List<Schema>>builder()
      .add(ImmutableList.of(Schema.OPTIONAL_INT32_SCHEMA))
      .add(ImmutableList.of(Schema.OPTIONAL_INT64_SCHEMA))
      .add(ImmutableList.of(Schema.OPTIONAL_FLOAT64_SCHEMA))
      .add(ImmutableList.of(Schema.OPTIONAL_STRING_SCHEMA))
      .add(ImmutableList.of(DecimalUtil.builder(1, 1)))
      .build();

  public TopkDistinctAggFunctionFactory() {
    super(NAME);
  }

  @SuppressWarnings("unchecked")
  @Override
  public KsqlAggregateFunction getProperAggregateFunction(final List<Schema> argTypeList) {
    if (argTypeList.isEmpty()) {
      throw new KsqlException("TOPKDISTINCT function should have two arguments.");
    }

    final Schema argSchema = argTypeList.get(0);
    switch (argSchema.type()) {
      case INT32:
      case INT64:
      case FLOAT64:
      case STRING:
        return new TopkDistinctKudaf(NAME, 0, -1, argSchema, SchemaUtil.getJavaType(argSchema));
      case BYTES:
        DecimalUtil.requireDecimal(argSchema);
        return new TopkDistinctKudaf(NAME, 0, -1, argSchema, SchemaUtil.getJavaType(argSchema));
      default:
        throw new KsqlException("No TOPKDISTINCT aggregate function with " + argTypeList.get(0)
            + " argument type exists!");
    }
  }

  @Override
  public List<List<Schema>> supportedArgs() {
    return SUPPORTED_TYPES;
  }
}