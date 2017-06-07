/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.function.udaf.count;

import io.confluent.ksql.function.KsqlAggFunctionDeterminer;
import io.confluent.ksql.function.KsqlAggregateFunction;
import org.apache.kafka.connect.data.Schema;

import java.util.Arrays;
import java.util.List;

public class CountAggFunctionDeterminer extends KsqlAggFunctionDeterminer {

  public CountAggFunctionDeterminer() {
    super("COUNT", Arrays.asList(new CountKudaf(-1)));
  }

  @Override
  public KsqlAggregateFunction getProperAggregateFunction(List<Schema> argTypeList) {
    return getAggregateFunctionList().get(0);
  }
}
