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

package io.confluent.ksql.function;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConstants;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.kafka.connect.data.Schema;


public abstract class AggregateFunctionFactory {

  private final UdfMetadata metadata;

  // used in most numeric functions
  protected static final List<List<Schema>> NUMERICAL_ARGS = ImmutableList
      .<List<Schema>>builder()
      .add(ImmutableList.of(Schema.OPTIONAL_INT32_SCHEMA))
      .add(ImmutableList.of(Schema.OPTIONAL_INT64_SCHEMA))
      .add(ImmutableList.of(Schema.OPTIONAL_FLOAT64_SCHEMA))
      .add(ImmutableList.of(DecimalUtil.builder(1, 1).build()))
      .build();

  public AggregateFunctionFactory(final String functionName) {
    this(new UdfMetadata(
        functionName,
        "",
        KsqlConstants.CONFLUENT_AUTHOR,
        "",
        KsqlFunction.INTERNAL_PATH,
        false
    ));
  }

  public AggregateFunctionFactory(final UdfMetadata metadata) {
    this.metadata = Objects.requireNonNull(metadata, "metadata can't be null");
  }

  public abstract KsqlAggregateFunction<?, ?, ?> createAggregateFunction(
      List<Schema> argTypeList, AggregateFunctionInitArguments initArgs);

  protected abstract List<List<Schema>> supportedArgs();

  public UdfMetadata getMetadata() {
    return metadata;
  }

  public String getName() {
    return metadata.getName();
  }

  public void eachFunction(final Consumer<KsqlAggregateFunction<?, ?, ?>> consumer) {
    supportedArgs().forEach(args ->
        consumer.accept(createAggregateFunction(args, getDefaultArguments())));
  }

  public AggregateFunctionInitArguments getDefaultArguments() {
    return AggregateFunctionInitArguments.EMPTY_ARGS;
  }

}
