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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlException;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public final class UdfUtil {

  private static final Map<Type, Supplier<SchemaBuilder>> typeToSchema
      = ImmutableMap.<Type, Supplier<SchemaBuilder>>builder()
      .put(String.class, () -> SchemaBuilder.string().optional())
      .put(boolean.class, SchemaBuilder::bool)
      .put(Boolean.class, () -> SchemaBuilder.bool().optional())
      .put(Integer.class, () -> SchemaBuilder.int32().optional())
      .put(int.class, SchemaBuilder::int32)
      .put(Long.class, () -> SchemaBuilder.int64().optional())
      .put(long.class, SchemaBuilder::int64)
      .put(Double.class, () -> SchemaBuilder.float64().optional())
      .put(double.class, SchemaBuilder::float64)
      .build();

  private UdfUtil() {

  }

  /**
   * Given the arguments and types for a function ensures the args are correct type.
   *
   * @param functionName The name of the function
   * @param args Argument array
   * @param argTypes Expected argument types
   */
  public static void ensureCorrectArgs(
      final String functionName,
      final Object[] args,
      final Class<?>... argTypes) {

    if (args == null) {
      throw new KsqlFunctionException(String.format("Null argument list for %s.", functionName));
    }

    if (args.length != argTypes.length) {
      throw new KsqlFunctionException(String.format("Incorrect arguments for %s.", functionName));
    }

    for (int i = 0; i < argTypes.length; i++) {
      if (args[i] == null) {
        continue;
      }

      if (!argTypes[i].isAssignableFrom(args[i].getClass())) {
        throw new KsqlFunctionException(
            String.format("Incorrect arguments type for %s. "
                + "Expected %s for arg number %d but found %s.",
                functionName,
                argTypes[i].getCanonicalName(),
                i,
                args[i].getClass().getCanonicalName()));
      }
    }
  }

  static Schema getSchemaFromType(final Type type) {
    return getSchemaFromType(type, null, null);
  }

  static Schema getSchemaFromType(final Type type, final String name, final String doc) {
    final SchemaBuilder schema;
    if (type instanceof TypeVariable) {
      schema = GenericsUtil.generic(((TypeVariable) type).getName());
    } else {
      schema = typeToSchema.getOrDefault(type, () -> handleParametrizedType(type)).get();
      schema.name(name);
    }

    schema.doc(doc);
    return schema.build();
  }

  private static SchemaBuilder handleParametrizedType(final Type type) {
    if (type instanceof ParameterizedType) {
      final ParameterizedType parameterizedType = (ParameterizedType) type;
      if (parameterizedType.getRawType() == Map.class) {
        final Schema keySchema = getSchemaFromType(parameterizedType.getActualTypeArguments()[0]);
        final Type valueType = ((ParameterizedType) type).getActualTypeArguments()[1];
        if (valueType instanceof TypeVariable) {
          return GenericsUtil.map(keySchema, ((TypeVariable) valueType).getName());
        }

        return SchemaBuilder.map(keySchema, getSchemaFromType(valueType));
      } else if (parameterizedType.getRawType() == List.class) {
        final Type valueType = ((ParameterizedType) type).getActualTypeArguments()[0];
        if (valueType instanceof TypeVariable) {
          return GenericsUtil.array(((TypeVariable) valueType).getName());
        }

        return SchemaBuilder.array(getSchemaFromType(valueType));
      }
    } else if (type instanceof Class<?> && ((Class<?>) type).isArray()) {
      // handle var args
      return SchemaBuilder.array(getSchemaFromType(((Class<?>) type).getComponentType()));
    } else if (type instanceof GenericArrayType) {
      return SchemaBuilder.array(
          GenericsUtil.generic(
              ((GenericArrayType) type).getGenericComponentType().getTypeName()
          ).build());
    }
    throw new KsqlException("Type inference is not supported for: " + type);
  }
}
