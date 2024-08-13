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
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.udaf.VariadicArgs;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.Quadruple;
import io.confluent.ksql.util.Quintuple;
import io.confluent.ksql.util.Triple;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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

  private static final ImmutableSet<Type> TUPLE_TYPES =
          ImmutableSet.<Type>builder()
                  .add(Pair.class)
                  .add(Triple.class)
                  .add(Quadruple.class)
                  .add(Quintuple.class)
                  .build();
  private static final Type VARIADIC_TYPE = VariadicArgs.class;

  private final boolean isVariadic;
  final int variadicColIndex;
  private final Type[] inputTypes;
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

    final Type inputWrapperType = type.getActualTypeArguments()[0];

    final boolean isMultipleArgs = TUPLE_TYPES.contains(getRawType(inputWrapperType));
    if (isMultipleArgs) {
      this.inputTypes = ((ParameterizedType) inputWrapperType).getActualTypeArguments();
    } else {
      this.inputTypes = new Type[]{inputWrapperType};
    }

    if (countVariadic(inputTypes, method) > 1) {
      throw new KsqlException("A UDAF and its factory can have at most one variadic argument");
    }

    variadicColIndex = indexOfType(inputTypes, VARIADIC_TYPE);
    if (method.isVarArgs()) {
      this.isVariadic = true;
    } else if (isMultipleArgs && variadicColIndex > -1) {
      this.isVariadic = true;
      this.inputTypes[variadicColIndex] = ((ParameterizedType) inputTypes[variadicColIndex])
              .getActualTypeArguments()[0];

    } else if (variadicColIndex > -1) {

      // Prevent zero column arguments
      throw new KsqlException("Variadic column arguments are only allowed inside tuples");

    } else {
      this.isVariadic = false;
    }

    // Disallow an object type outside a variadic column
    final boolean hasMultipleObjectArgs = countTypes(inputTypes, Object.class) > 1;
    final int indexOfFirstObj = indexOfType(inputTypes, Object.class);
    final boolean objArgIsNotVariadic = indexOfFirstObj >= 0 && indexOfFirstObj != variadicColIndex;
    if (hasMultipleObjectArgs || objArgIsNotVariadic) {
      throw new KsqlException("The Object type can only be used as a variadic column argument");
    }

    this.aggregateType = type.getActualTypeArguments()[1];
    this.outputType = type.getActualTypeArguments()[2];

    this.literalParams = FunctionLoaderUtils
        .createParameters(method, functionName.text(), sqlTypeParser);

    validateTypes(inputTypes, variadicColIndex);
    validateType(aggregateType, false);
    validateType(outputType, false);
  }

  List<ParameterInfo> getInputSchema(final String[] inSchemas) {
    final List<ParamType> paramTypes = new ArrayList<>();

    for (int paramIndex = 0; paramIndex < inputTypes.length; paramIndex++) {
      final Type inputType = inputTypes[paramIndex];
      final String schema = paramIndex < inSchemas.length ? inSchemas[paramIndex] : "";
      Objects.requireNonNull(schema);

      validateStructAnnotation(inputType, schema, "paramSchema");

      final ParamType paramType;
      if (paramIndex == variadicColIndex) {
        paramType = ArrayType.of(getSchemaFromType(inputType, schema, true));
      } else {
        paramType = getSchemaFromType(inputType, schema, false);
      }

      paramTypes.add(paramType);
    }

    final List<ParameterInfo> paramInfos = IntStream.range(0, paramTypes.size())
            .mapToObj((paramIndex) -> {
              final ParamType paramType = paramTypes.get(paramIndex);

              return new ParameterInfo(
                      "val" + (paramIndex + 1),
                      paramType,
                      "",
                      paramIndex == variadicColIndex
              );
            }).collect(Collectors.toList());

    return ImmutableList.<ParameterInfo>builder()
        .addAll(paramInfos)
        .addAll(literalParams)
        .build();
  }

  ParamType getAggregateSchema(final String aggSchema) {
    validateStructAnnotation(aggregateType, aggSchema, "aggregateSchema");
    return getSchemaFromType(aggregateType, aggSchema, false);
  }

  ParamType getOutputSchema(final String outSchema) {
    validateStructAnnotation(outputType, outSchema, "returnSchema");
    return getSchemaFromType(outputType, outSchema, false);
  }

  boolean isVariadic() {
    return isVariadic;
  }

  List<ParameterInfo> literalParams() {
    return ImmutableList.copyOf(literalParams);
  }

  private void validateType(final Type t, final boolean isVariadic) {
    if (!(t instanceof TypeVariable)
            && isUnsupportedType((Class<?>) getRawType(t), isVariadic)) {

      throw new KsqlException(String.format(invalidClassErrorMsg, t));
    }
  }

  private void validateTypes(final Type[] types, final int variadicColIndex) {
    for (int index = 0; index < types.length; index++) {
      validateType(types[index], index == variadicColIndex);
    }
  }

  private static long countVariadic(final Type[] types, final Method factory) {
    long count = countTypes(types, VARIADIC_TYPE);

    /* If there is a variadic initial argument, include it in the total number of variadic
    arguments. We need to include this because there can only be one variadic argument in
    the function signature, whether it is a column argument or an initial argument. */
    if (factory.isVarArgs()) {
      count++;
    }

    return count;
  }

  private static long countTypes(final Type[] types, final Type matchType) {
    return Arrays.stream(types).filter((type) -> getRawType(type).equals(matchType)).count();
  }

  private static int indexOfType(final Type[] types, final Type matchType) {
    return IntStream.range(0, types.length)
            .filter((index) -> getRawType(types[index]).equals(matchType))
            .findFirst()
            .orElse(-1);
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

  private ParamType getSchemaFromType(final Type type, final String schema,
                                      final boolean isVariadic) {
    if (schema.isEmpty()) {
      return isVariadic ? UdfUtil.getVarArgsSchemaFromType(type) : UdfUtil.getSchemaFromType(type);
    }

    return SchemaConverters.sqlToFunctionConverter().toFunctionType(
            sqlTypeParser.parse(schema).getSqlType()
    );
  }

  private static Type getRawType(final Type type) {
    if (type instanceof ParameterizedType) {
      return ((ParameterizedType) type).getRawType();
    }
    return type;
  }

  @SuppressWarnings("checkstyle:BooleanExpressionComplexity")
  private static boolean isUnsupportedType(final Class<?> type, final boolean allowObject) {
    return !SUPPORTED_TYPES.contains(type)
        && (!type.isArray() || !SUPPORTED_TYPES.contains(type.getComponentType()))
        && SUPPORTED_TYPES.stream().noneMatch(supported -> supported.isAssignableFrom(type))
        && (!allowObject || !type.equals(Object.class));
  }

  static void checkSupportedType(final Method method, final Class<?> type) {
    if (UdafTypes.isUnsupportedType(type, false)) {
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
