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

import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.List;
import org.apache.kafka.connect.data.Schema;


public interface KsqlTableFunction<I, O> extends FunctionSignature {

  KsqlTableFunction<I, O> getInstance(TableFunctionArguments tableFunctionArguments);

  Schema getReturnType();

  SqlType returnType();

  boolean hasSameArgTypes(List<Schema> argTypeList);

  List<O> flatMap(I currentValue);

  String getDescription();

  @Override
  default boolean isVariadic() {
    return false;
  }
}
