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

package io.confluent.ksql.function.udaf.topk;

import org.apache.kafka.connect.data.Schema;

import java.util.Arrays;
import java.util.List;

import io.confluent.ksql.function.KsqlAggFunctionDeterminer;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.connect.data.SchemaBuilder;

public class TopkAggFunctionDeterminer extends KsqlAggFunctionDeterminer {
    private int topKSize = 0;

    public TopkAggFunctionDeterminer() {
        super("TOPK", Arrays.asList());
    }

    public TopkAggFunctionDeterminer(int topKSize) {
        this();
        this.topKSize = topKSize;
    }

  @Override
  public KsqlAggregateFunction getProperAggregateFunction(List<Schema> argTypeList) {
    if (argTypeList.isEmpty()) {
      throw new KsqlException("TOPK function should have two arguments.");
    }
    Schema argSchema = argTypeList.get(0);
    switch (argSchema.type()) {
      case INT32:
        return new TopkKudaf<Integer>(
                -1,
                topKSize,
                SchemaBuilder.array(Schema.INT32_SCHEMA).build(),
                Arrays.asList(Schema.INT32_SCHEMA),
                Integer.class);
      case INT64:
        return new TopkKudaf<Long>(
                -1,
                topKSize,
                SchemaBuilder.array(Schema.INT64_SCHEMA).build(),
                Arrays.asList(Schema.INT64_SCHEMA),
                Long.class);
      case FLOAT64:
        return new TopkKudaf<Double>(
                -1,
                topKSize,
                SchemaBuilder.array(Schema.FLOAT64_SCHEMA).build(),
                Arrays.asList(Schema.FLOAT64_SCHEMA),
                Double.class);
      case STRING:
        return new TopkKudaf<String>(
                -1,
                topKSize,
                SchemaBuilder.array(Schema.STRING_SCHEMA).build(),
                Arrays.asList(Schema.STRING_SCHEMA),
                String.class);
      default:
        throw new KsqlException("No TOPK aggregate function with " + argTypeList.get(0)
                                + " argument type exists!");
    }
  }
}
