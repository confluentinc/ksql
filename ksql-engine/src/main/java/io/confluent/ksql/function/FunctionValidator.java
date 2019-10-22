/*
 * Copyright 2019 Confluent Inc.
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

import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Validates UDF/UDAFs.
 */
final class FunctionValidator {

  void validateReturnType(final Type actualType, final SqlType expectedType) {
    try {
      validateType(actualType, expectedType);
    } catch (final Exception e) {
      throw new KsqlException(
          "Return type does not match supplied "
              + "schema: " + expectedType + ". " + e.getMessage(),
          e
      );
    }
  }

  private void validateType(final Type actualType, final SqlType expectedType) {
    validateActualType(actualType);

    switch (expectedType.baseType()) {
      case ARRAY:
        validateArrayReturnType(actualType, (SqlArray) expectedType);
        return;
      case MAP:
        validateMapReturnType(actualType, (SqlMap) expectedType);
        return;
      default:
        final Class<?> expectedJavaType = SchemaConverters.sqlToJavaConverter()
            .toJavaType(expectedType.baseType());

        if (!actualType.equals(expectedJavaType)) {
          throw new TypeMismatchException(expectedJavaType, actualType);
        }
    }
  }

  private void validateArrayReturnType(final Type actualType, final SqlArray expectedType) {
    final Optional<ParameterizedType> pt = findParameterizedType(actualType, List.class);
    if (!pt.isPresent()) {
      return;
    }

    final ParameterizedType parameterized = pt.get();
    final Type itemType = parameterized.getActualTypeArguments()[0];

    try {
      validateType(itemType, expectedType.getItemType());
    } catch (final TypeMismatchException | UnsupportedOperationException e) {
      throw new KsqlException("Type mismatch on element type. " + e.getMessage(), e);
    }
  }

  private void validateMapReturnType(final Type actualType, final SqlMap expectedType) {
    final Optional<ParameterizedType> pt = findParameterizedType(actualType, Map.class);
    if (!pt.isPresent()) {
      return;
    }

    final ParameterizedType parameterized = pt.get();

    final Type keyType = parameterized.getActualTypeArguments()[0];
    if (!keyType.equals(String.class)) {
      throw new KsqlException(
          "Type mismatch on map key type. expected: " + String.class + ", but got: " + keyType);
    }

    final Type valueType = parameterized.getActualTypeArguments()[1];

    try {
      validateType(valueType, expectedType.getValueType());
    } catch (final TypeMismatchException | UnsupportedOperationException e) {
      throw new KsqlException("Type mismatch on map value type. " + e.getMessage(), e);
    }
  }

  private static void validateActualType(final Type actualType) {
    if (actualType instanceof WildcardType) {
      throw new UnsupportedOperationException(
          "Wildcards, i.e. '?', are not supported. Please use specific types.");
    }

    if (actualType instanceof TypeVariable) {
      throw new UnsupportedOperationException(
          "Type variables, e.g. " + actualType + ", are not supported. "
              + "Please use specific types.");
    }

    if (actualType instanceof GenericArrayType) {
      throw new UnsupportedOperationException(
          "Generic array types, e.g. " + actualType + ", are not supported. "
              + "Please List<> for ARRAY types.");
    }
  }

  private static Optional<ParameterizedType> findParameterizedType(
      final Type actualType,
      final Class<?> required
  ) {
    if (actualType instanceof Class) {
      final Class<?> actualClass = (Class<?>) actualType;

      if (!required.equals(actualClass)) {
        throw new TypeMismatchException(required, actualType);
      }

      return Optional.empty();
    }

    final ParameterizedType parameterized = (ParameterizedType) actualType;
    if (!required.isAssignableFrom((Class<?>) parameterized.getRawType())) {
      throw new TypeMismatchException(required, actualType);
    }

    return Optional.of(parameterized);
  }

  private static final class TypeMismatchException extends RuntimeException {

    TypeMismatchException(final Type expected, final Type actual) {
      super("expected: " + expected + ", but got: " + actual);
    }
  }
}
