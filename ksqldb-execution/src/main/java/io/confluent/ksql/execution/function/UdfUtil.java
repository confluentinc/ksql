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

package io.confluent.ksql.execution.function;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.codegen.helpers.TriFunction;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.GenericType;
import io.confluent.ksql.function.types.LambdaType;
import io.confluent.ksql.function.types.MapType;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.function.types.StructType;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.util.KsqlException;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.kafka.connect.data.Struct;

public final class UdfUtil {

  private static final Map<Type, ParamType> JAVA_TO_ARG_TYPE
      = ImmutableMap.<Type, ParamType>builder()
      .put(String.class, ParamTypes.STRING)
      .put(boolean.class, ParamTypes.BOOLEAN)
      .put(Boolean.class, ParamTypes.BOOLEAN)
      .put(Integer.class, ParamTypes.INTEGER)
      .put(int.class, ParamTypes.INTEGER)
      .put(Long.class, ParamTypes.LONG)
      .put(long.class, ParamTypes.LONG)
      .put(Double.class, ParamTypes.DOUBLE)
      .put(double.class, ParamTypes.DOUBLE)
      .put(BigDecimal.class, ParamTypes.DECIMAL)
      .put(Timestamp.class, ParamTypes.TIMESTAMP)
      .put(TimeUnit.class, ParamTypes.INTERVALUNIT)
      .build();

  private UdfUtil() {

  }

  /**
   * Given the arguments and types for a function ensures the args are correct type.
   *
   * @param functionName The name of the function
   * @param args         Argument array
   * @param argTypes     Expected argument types
   */
  public static void ensureCorrectArgs(
      final FunctionName functionName, final Object[] args, final Class<?>... argTypes
  ) {
    if (args == null) {
      throw new KsqlFunctionException("Null argument list for " + functionName.text() + ".");
    }

    if (args.length != argTypes.length) {
      throw new KsqlFunctionException("Incorrect arguments for " + functionName.text() + ".");
    }

    for (int i = 0; i < argTypes.length; i++) {
      if (args[i] == null) {
        continue;
      }

      if (!argTypes[i].isAssignableFrom(args[i].getClass())) {
        throw new KsqlFunctionException(
            String.format(
                "Incorrect arguments type for %s. "
                    + "Expected %s for arg number %d but found %s.",
                functionName.text(),
                argTypes[i].getCanonicalName(),
                i,
                args[i].getClass().getCanonicalName()
            ));
      }
    }
  }

  public static ParamType getSchemaFromType(final Type type) {
    ParamType schema;
    if (type instanceof TypeVariable) {
      schema = GenericType.of(((TypeVariable) type).getName());
    } else {
      schema = JAVA_TO_ARG_TYPE.get(type);
      if (schema == null) {
        schema = handleParameterizedType(type);
      }
    }

    return schema;
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  private static ParamType handleParameterizedType(final Type type) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    if (type instanceof ParameterizedType) {
      final ParameterizedType parameterizedType = (ParameterizedType) type;
      if (parameterizedType.getRawType() == Map.class) {
        return handleMapType(parameterizedType);
      } else if (parameterizedType.getRawType() == List.class) {
        return handleListType((ParameterizedType) type);
      }
      if (parameterizedType.getRawType() == Function.class
          || parameterizedType.getRawType() == BiFunction.class
          || parameterizedType.getRawType() == TriFunction.class) {
        return handleLambdaType((ParameterizedType) type);
      }
    } else if (type instanceof Class<?> && ((Class<?>) type).isArray()) {
      // handle var args
      return ArrayType.of(getSchemaFromType(((Class<?>) type).getComponentType()));
    } else if (type instanceof GenericArrayType) {
      return ArrayType.of(
          GenericType.of(
              ((GenericArrayType) type).getGenericComponentType().getTypeName()));
    } else if (type instanceof Class<?> && Struct.class.isAssignableFrom((Class<?>) type)) {
      // we don't have enough information here to return a more specific type of struct,
      // but there are other parts of the code that enforce having a schema provider or
      // schema annotation if a struct is being used
      return StructType.ANY_STRUCT;
    }
    throw new KsqlException("Type inference is not supported for: " + type);
  }

  private static ParamType handleMapType(final ParameterizedType type) {
    final Type keyType = type.getActualTypeArguments()[0];
    final ParamType keyParamType = keyType instanceof TypeVariable
        ? GenericType.of(((TypeVariable<?>) keyType).getName())
        : getSchemaFromType(keyType);

    final Type valueType = type.getActualTypeArguments()[1];
    final ParamType valueParamType = valueType instanceof TypeVariable
        ? GenericType.of(((TypeVariable<?>) valueType).getName())
        : getSchemaFromType(valueType);

    return MapType.of(keyParamType, valueParamType);
  }

  private static ParamType handleListType(final ParameterizedType type) {
    final Type elementType = type.getActualTypeArguments()[0];
    final ParamType elementParamType = elementType instanceof TypeVariable
        ? GenericType.of(((TypeVariable<?>) elementType).getName())
        : getSchemaFromType(elementType);

    return ArrayType.of(elementParamType);
  }

  private static ParamType handleLambdaType(final ParameterizedType type) {
    final List<ParamType> inputParamTypes = new ArrayList<>();
    for (int i = 0; i < type.getActualTypeArguments().length - 1; i++) {
      final Type inputType = type.getActualTypeArguments()[i];
      final ParamType inputParamType = inputType instanceof TypeVariable
          ? GenericType.of(((TypeVariable<?>) inputType).getName())
          : getSchemaFromType(inputType);
      inputParamTypes.add(inputParamType);
    }

    final Type returnType =
        type.getActualTypeArguments()[type.getActualTypeArguments().length - 1];
    final ParamType returnParamType = returnType instanceof TypeVariable
        ? GenericType.of(((TypeVariable<?>) returnType).getName())
        : getSchemaFromType(returnType);

    return LambdaType.of(inputParamTypes, returnParamType);
  }
}
