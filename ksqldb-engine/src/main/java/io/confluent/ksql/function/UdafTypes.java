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
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.execution.codegen.helpers.TriFunction;
import io.confluent.ksql.execution.function.UdfUtil;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.util.KsqlException;
import java.lang.reflect.AnnotatedParameterizedType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.kafka.connect.data.Struct;

class UdafTypes {

  private static final Set<Class<?>> SUPPORTED_TYPES = ImmutableSet.<Class<?>>builder()
      .add(int.class)
      .add(long.class)
      .add(double.class)
      .add(boolean.class)
      .add(Integer.class)
      .add(Long.class)
      .add(Double.class)
      .add(BigDecimal.class)
      .add(Boolean.class)
      .add(String.class)
      .add(Struct.class)
      .add(List.class)
      .add(Map.class)
      .add(Date.class)
      .add(Time.class)
      .add(Timestamp.class)
      .add(TimeUnit.class)
      .add(Function.class)
      .add(BiFunction.class)
      .add(TriFunction.class)
      .add(ByteBuffer.class)
      .build();

  private final Type inputType;
  private final Type aggregateType;
  private final Type outputType;
  private final List<ParameterInfo> literalParams;
  private final String invalidClassErrorMsg;
  private final SqlTypeParser sqlTypeParser;

  UdafTypes(
      final Method method,
      final FunctionName functionName,
      final SqlTypeParser sqlTypeParser
  ) {
    this.invalidClassErrorMsg = "class='%s'"
        + " is not supported by UDAFs. Valid types are: " + SUPPORTED_TYPES + " "
        + Objects.requireNonNull(functionName, "functionName");
    final AnnotatedParameterizedType annotatedReturnType
        = (AnnotatedParameterizedType) method.getAnnotatedReturnType();
    final ParameterizedType type = (ParameterizedType) annotatedReturnType.getType();
    this.sqlTypeParser = Objects.requireNonNull(sqlTypeParser);

    this.inputType = type.getActualTypeArguments()[0];
    this.aggregateType = type.getActualTypeArguments()[1];
    this.outputType = type.getActualTypeArguments()[2];

    this.literalParams = FunctionLoaderUtils
        .createParameters(method, functionName.text(), sqlTypeParser);

    validateTypes(inputType);
    validateTypes(aggregateType);
    validateTypes(outputType);
  }

  List<ParameterInfo> getInputSchema(final String inSchema) {
    validateStructAnnotation(inputType, inSchema, "paramSchema");
    final ParamType inputSchema = getSchemaFromType(inputType, inSchema);

    return ImmutableList.<ParameterInfo>builder()
        .add(new ParameterInfo("val", inputSchema, "", false))
        .addAll(literalParams)
        .build();
  }

  ParamType getAggregateSchema(final String aggSchema) {
    validateStructAnnotation(aggregateType, aggSchema, "aggregateSchema");
    return getSchemaFromType(aggregateType, aggSchema);
  }

  ParamType getOutputSchema(final String outSchema) {
    validateStructAnnotation(outputType, outSchema, "returnSchema");
    return getSchemaFromType(outputType, outSchema);
  }

  private void validateTypes(final Type t) {
    if (!(t instanceof TypeVariable) && isUnsupportedType((Class<?>) getRawType(t))) {
      throw new KsqlException(String.format(invalidClassErrorMsg, t));
    }
  }

  private static void validateStructAnnotation(
      final Type type,
      final String schema,
      final String msg
  ) {
    if (type.equals(Struct.class) && schema.isEmpty()) {
      throw new KsqlException("Must specify '" + msg + "' for STRUCT parameter in @UdafFactory or"
          + " implement getAggregateSqlType()/getReturnSqlType().");
    }
  }

  private ParamType getSchemaFromType(final Type type, final String schema) {
    return schema.isEmpty()
        ? UdfUtil.getSchemaFromType(type)
        : SchemaConverters.sqlToFunctionConverter().toFunctionType(
            sqlTypeParser.parse(schema).getSqlType());
  }

  private static Type getRawType(final Type type) {
    if (type instanceof ParameterizedType) {
      return ((ParameterizedType) type).getRawType();
    }
    return type;
  }

  static boolean isUnsupportedType(final Class<?> type) {
    return !SUPPORTED_TYPES.contains(type)
        && (!type.isArray() || !SUPPORTED_TYPES.contains(type.getComponentType()))
        && SUPPORTED_TYPES.stream().noneMatch(supported -> supported.isAssignableFrom(type));
  }

  static void checkSupportedType(final Method method, final Class<?> type) {
    if (UdafTypes.isUnsupportedType(type)) {
      throw new KsqlException(
          String.format(
              "Type %s is not supported by UDF methods. "
                  + "Supported types %s. method=%s, class=%s",
              type,
              SUPPORTED_TYPES,
              method.getName(),
              method.getDeclaringClass()
          )
      );
    }
  }

}
