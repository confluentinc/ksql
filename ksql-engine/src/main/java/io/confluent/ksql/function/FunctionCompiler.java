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

package io.confluent.ksql.function;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.execution.function.UdfUtil;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlTypeParser;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public abstract class FunctionCompiler {

  static final Set<Class<?>> SUPPORTED_TYPES = ImmutableSet.<Class<?>>builder()
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
      .build();

  private static final SqlTypeParser typeParser = SqlTypeParser.create(TypeRegistry.EMPTY);

  static void validateMethodSignature(final Method method, final String functionName) {
  }

  static void validateParameters(final Method method) {
  }

  static Schema parseParameter(final Type type,
                                       final Optional<String> schemaString,
                                       final String msg,
                                       final String name,
                                       final String doc) {

    validateStructAnnotation(type, schemaString, msg);
    return SchemaUtil.ensureOptional(getSchemaFromInputParameter(type, schemaString, name, doc));
  }

  static void validateStructAnnotation(final Type type,
                                               final Optional<String> schema,
                                               final String msg) {

    if (type.equals(Struct.class) && !schema.isPresent()) {
      throw new KsqlException(String.format("Must specify '%s' for STRUCT parameter in "
                                                + "@UdafFactory.", msg));
    }
  }

  static Type getRawType(final Type type) {
    if (type instanceof ParameterizedType) {
      return ((ParameterizedType) type).getRawType();
    }
    return type;
  }

  static Schema getSchemaFromInputParameter(final Type type,
                                                    final Optional<String> paramSchema,
                                                    final String paramName,
                                                    final String paramDoc) {

    Objects.requireNonNull(paramName);
    Objects.requireNonNull(paramDoc);
    if (paramSchema.isPresent()) {
      return SchemaConverters.sqlToConnectConverter().toConnectSchema(
          typeParser.parse(paramSchema.get()).getSqlType(), paramName, paramDoc);
    }

    return UdfUtil.getSchemaFromType(type, paramName, paramDoc);
  }

  static boolean isUnsupportedType(final Class<?> type) {
    return !SUPPORTED_TYPES.contains(type)
        && (!type.isArray() || !SUPPORTED_TYPES.contains(type.getComponentType()))
        && SUPPORTED_TYPES.stream().noneMatch(supported -> supported.isAssignableFrom(type));
  }


}
