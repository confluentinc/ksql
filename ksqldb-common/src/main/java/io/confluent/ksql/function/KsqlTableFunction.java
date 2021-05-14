/*
 * Copyright 2021 Confluent Inc.
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

import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.util.List;
import java.util.Objects;

/**
 * A wrapper around the actual table function which provides methods to get return type and
 * description, and allows the function to be invoked.
 */
public class KsqlTableFunction extends KsqlFunction {

  @EffectivelyImmutable
  private final Kudf udtf;

  public KsqlTableFunction(
      final SchemaProvider returnSchemaProvider,
      final FunctionName functionName,
      final ParamType outputType,
      final List<ParameterInfo> parameters,
      final String description,
      final Kudf udtf
  ) {
    super(returnSchemaProvider, outputType, parameters, functionName, description,
        "", false
    );
    this.udtf = Objects.requireNonNull(udtf, "udtf");
  }

  public List<?> apply(final Object... args) {
    return (List<?>) udtf.evaluate(args);
  }
}
