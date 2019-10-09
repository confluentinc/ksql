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

import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;

public class UdtfTableFunctionFactory extends TableFunctionFactory {
  private final UdfIndex<KsqlTableFunction<?, ?>> udfIndex;

  UdtfTableFunctionFactory(
      final UdfMetadata metadata,
      final List<KsqlTableFunction<?, ?>> functionList
  ) {
    super(metadata);
    udfIndex = new UdfIndex<>(metadata.getName());
    functionList.forEach(udfIndex::addFunction);
  }

  @Override
  public KsqlTableFunction<?, ?> getProperTableFunction(final List<Schema> argTypeList) {
    final KsqlTableFunction ksqlTableFunction = udfIndex.getFunction(argTypeList);
    if (ksqlTableFunction == null) {
      throw new KsqlException("There is no table function with name='" + getName()
          + "' that has arguments of type="
          + argTypeList.stream().map(schema -> schema.type().getName())
          .collect(Collectors.joining(",")));
    }
    return ksqlTableFunction;
  }

  @Override
  public List<List<Schema>> supportedArgs() {
    return udfIndex.values()
        .stream()
        .map(KsqlTableFunction::getArguments)
        .collect(Collectors.toList());
  }
}
