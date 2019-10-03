/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.codegen;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.function.KsqlFunction;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.util.KsqlConfig;

@Immutable
public final class FunctionSpec {
  private final Class type;
  private final KsqlFunction function;
  private final String codegenName;
  private final KsqlConfig ksqlConfig;

  FunctionSpec(
      final KsqlFunction function,
      final String codegenName,
      final KsqlConfig ksqlConfig
  ) {
    this.type = function.getKudfClass();
    this.function = function;
    this.codegenName = codegenName;
    this.ksqlConfig = ksqlConfig;
  }

  public Class type() {
    return type;
  }

  public String codeGenName() {
    return codegenName;
  }

  public FunctionName functionName() {
    return function.getFunctionName();
  }

  public Kudf newInstance() {
    return function.newInstance(ksqlConfig);
  }
}
