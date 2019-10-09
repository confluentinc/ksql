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

package io.confluent.ksql.function.udtf.array;

import io.confluent.ksql.function.BaseTableFunction;
import io.confluent.ksql.function.KsqlTableFunction;
import io.confluent.ksql.function.TableFunctionArguments;
import io.confluent.ksql.name.FunctionName;
import java.util.List;
import org.apache.kafka.connect.data.Schema;

public class ExplodeIntegerArrayUdtf extends BaseTableFunction<List<Integer>, Integer> {

  public ExplodeIntegerArrayUdtf(
      final FunctionName functionName,
      final Schema outputType,
      final List<Schema> arguments,
      final String description) {
    super(functionName, outputType, arguments, description);
  }

  @Override
  public KsqlTableFunction<List<Integer>, Integer> getInstance(
      final TableFunctionArguments tableFunctionArguments) {
    return new ExplodeIntegerArrayUdtf(functionName, getReturnType(),
        getArguments(), getDescription());
  }

  @Override
  public List<Integer> flatMap(final List<Integer> currentValue) {
    return currentValue;
  }

}
