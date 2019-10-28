/*
 * Copyright 2019 Confluent Inc.
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

import io.confluent.ksql.function.udf.UdfMetadata;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;

/**
 * A table function factory used for creating user defined table functions.
 */
public class UdtfTableFunctionFactory extends TableFunctionFactory {

  private final UdfIndex<KsqlTableFunction> udtfIndex;

  public UdtfTableFunctionFactory(final UdfMetadata metadata) {
    super(metadata);
    this.udtfIndex = new UdfIndex<>(metadata.getName());
  }

  @Override
  public KsqlTableFunction createTableFunction(final List<Schema> argTypeList) {
    return udtfIndex.getFunction(argTypeList);
  }

  @Override
  protected List<List<Schema>> supportedArgs() {
    return udtfIndex.values()
        .stream()
        .map(KsqlTableFunction::getArguments)
        .collect(Collectors.toList());
  }

  void addFunction(final KsqlTableFunction tableFunction) {
    udtfIndex.addFunction(tableFunction);
  }

}
