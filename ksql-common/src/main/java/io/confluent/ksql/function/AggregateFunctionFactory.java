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

package io.confluent.ksql.function;

import io.confluent.ksql.function.udf.UdfMetadata;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.kafka.connect.data.Schema;


public abstract class AggregateFunctionFactory {

  private final List<KsqlAggregateFunction<?, ?>> aggregateFunctionList;
  private final UdfMetadata metadata;

  public AggregateFunctionFactory(final String functionName,
                                  final List<KsqlAggregateFunction<?, ?>> aggregateFunctionList) {
    this(new UdfMetadata(functionName, "", "confluent", "", KsqlFunction.INTERNAL_PATH),
        aggregateFunctionList);
  }

  public AggregateFunctionFactory(final UdfMetadata metadata,
                                  final List<KsqlAggregateFunction<?, ?>> aggregateFunctionList) {
    this.metadata = Objects.requireNonNull(metadata, "metadata can't be null");
    this.aggregateFunctionList = Objects.requireNonNull(aggregateFunctionList,
        "aggregateFunctionList can't be null");
  }

  public abstract KsqlAggregateFunction getProperAggregateFunction(List<Schema> argTypeList);

  public String getName() {
    return metadata.getName();
  }

  public String getDescription() {
    return metadata.getDescription();
  }

  public String getPath() {
    return metadata.getPath();
  }

  public String getAuthor() {
    return metadata.getAuthor();
  }

  public String getVersion() {
    return metadata.getVersion();
  }

  public void eachFunction(final Consumer<KsqlAggregateFunction<?, ?>> consumer) {
    aggregateFunctionList.forEach(consumer);
  }

  protected List<KsqlAggregateFunction<?, ?>> getAggregateFunctionList() {
    return aggregateFunctionList;
  }
}
