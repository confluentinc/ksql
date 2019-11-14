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
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.GenericType;
import io.confluent.ksql.function.types.MapType;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.function.types.StringType;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.util.KsqlException;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

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
      FunctionName functionName, Object[] args, Class<?>... argTypes
  ) {
    if (args == null) {
      throw new KsqlFunctionException("Null argument list for " + functionName.name() + ".");
    }

    if (args.length != argTypes.length) {
      throw new KsqlFunctionException("Incorrect arguments for " + functionName.name() + ".");
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
                functionName.name(),
                argTypes[i].getCanonicalName(),
                i,
                args[i].getClass().getCanonicalName()
            ));
      }
    }
  }

  public static ParamType getSchemaFromType(Type type) {
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

  private static ParamType handleParameterizedType(Type type) {
    if (type instanceof ParameterizedType) {
      ParameterizedType parameterizedType = (ParameterizedType) type;
      if (parameterizedType.getRawType() == Map.class) {
        ParamType keyType = getSchemaFromType(parameterizedType.getActualTypeArguments()[0]);
        if (!(keyType instanceof StringType)) {
          throw new KsqlException("Maps only support STRING keys, got: " + keyType);
        }
        Type valueType = ((ParameterizedType) type).getActualTypeArguments()[1];
        if (valueType instanceof TypeVariable) {
          return MapType.of(GenericType.of(((TypeVariable) valueType).getName()));
        }

        return MapType.of(getSchemaFromType(valueType));
      } else if (parameterizedType.getRawType() == List.class) {
        Type valueType = ((ParameterizedType) type).getActualTypeArguments()[0];
        if (valueType instanceof TypeVariable) {
          return ArrayType.of(GenericType.of(((TypeVariable) valueType).getName()));
        }

        return ArrayType.of(getSchemaFromType(valueType));
      }
    } else if (type instanceof Class<?> && ((Class<?>) type).isArray()) {
      // handle var args
      return ArrayType.of(getSchemaFromType(((Class<?>) type).getComponentType()));
    } else if (type instanceof GenericArrayType) {
      return ArrayType.of(
          GenericType.of(
              ((GenericArrayType) type).getGenericComponentType().getTypeName()));
    }

    throw new KsqlException("Type inference is not supported for: " + type);
  }
}
