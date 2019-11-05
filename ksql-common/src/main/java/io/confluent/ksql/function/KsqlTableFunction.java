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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.name.FunctionName;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;

/**
 * A wrapper around the actual table function which provides methods to get return type and
 * description, and allows the function to be invoked.
 */
@Immutable
public class KsqlTableFunction extends KsqlFunction {

  private final Kudf udtf;

  public KsqlTableFunction(
      final Function<List<Schema>, Schema> returnSchemaProvider,
      final FunctionName functionName,
      final Schema outputType,
      final List<Schema> arguments,
      final String description,
      final Kudf udtf
  ) {
    super(returnSchemaProvider, outputType, arguments, functionName, description,
        "", false
    );
    this.udtf = Objects.requireNonNull(udtf, "udtf");
  }

  public List<?> apply(final Object... args) {
    return (List<?>) udtf.evaluate(args);
  }
}
