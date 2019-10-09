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

import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SchemaConverters.ConnectToSqlTypeConverter;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.connect.data.Schema;

public abstract class BaseTableFunction<I, O> implements KsqlTableFunction<I, O> {

  private static final ConnectToSqlTypeConverter CONNECT_TO_SQL_CONVERTER
      = SchemaConverters.connectToSqlConverter();

  private final Schema outputSchema;
  private final SqlType outputType;
  private final List<Schema> arguments;

  protected final FunctionName functionName;
  private final String description;

  public BaseTableFunction(
      final FunctionName functionName,
      final Schema outputType,
      final List<Schema> arguments,
      final String description
  ) {
    this.outputSchema = Objects.requireNonNull(outputType, "outputType");
    this.outputType = CONNECT_TO_SQL_CONVERTER.toSqlType(outputType);
    this.arguments = Objects.requireNonNull(arguments, "arguments");
    this.functionName = Objects.requireNonNull(functionName, "functionName");
    this.description = Objects.requireNonNull(description, "description");

    if (!outputType.isOptional()) {
      throw new IllegalArgumentException("KSQL only supports optional field types");
    }
  }

  public boolean hasSameArgTypes(final List<Schema> argTypeList) {
    if (argTypeList == null) {
      throw new KsqlException("Argument type list is null.");
    }
    return this.arguments.equals(argTypeList);
  }

  public FunctionName getFunctionName() {
    return functionName;
  }

  public Schema getReturnType() {
    return outputSchema;
  }

  @Override
  public SqlType returnType() {
    return outputType;
  }

  public List<Schema> getArguments() {
    return arguments;
  }

  @Override
  public String getDescription() {
    return description;
  }
}
