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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.GenericType;
import io.confluent.ksql.function.types.LambdaType;
import io.confluent.ksql.function.types.MapType;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.types.StructType;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlLambda;
import io.confluent.ksql.schema.ksql.types.SqlLambdaResolved;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlStruct.Builder;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlPreconditions;
import io.confluent.ksql.util.Pair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public final class GenericsUtil {

  private GenericsUtil() {
  }

  /**
   * @param type  the type
   * @return whether or not {@code type} is a generic type
   * @apiNote container generics (e.g. {@code ARRAY<T>}) will return {@code false},
   *          use {@link #constituentGenerics(ParamType)}
   */
  public static boolean isGeneric(final ParamType type) {
    return type instanceof GenericType;
  }

  /**
   * @param type  the type
   * @return all generics contained within the type, for example: {@code Map<K, V>} would return
   *         a set containing {@code {<K>, <V>}}
   */
  public static Set<ParamType> constituentGenerics(final ParamType type) {
    if (type instanceof ArrayType) {
      return constituentGenerics(((ArrayType) type).element());
    } else if (type instanceof MapType) {
      return Sets.union(
          constituentGenerics(((MapType) type).key()),
          constituentGenerics(((MapType) type).value()));
    } else if (type instanceof StructType) {
      return ((StructType) type).getSchema()
          .values()
          .stream()
          .map(GenericsUtil::constituentGenerics)
          .flatMap(Collection::stream)
          .collect(Collectors.toSet());
    } else if (type instanceof GenericType) {
      return ImmutableSet.of(type);
    } else if (type instanceof LambdaType) {
      final Set<ParamType> inputSet = new HashSet<>();
      for (final ParamType paramType: ((LambdaType) type).inputTypes()) {
        inputSet.addAll(constituentGenerics(paramType));
      }
      return Sets.union(
          inputSet,
          constituentGenerics(((LambdaType) type).returnType()));
    } else {
      return ImmutableSet.of();
    }
  }

  /**
   * @param type  the schema
   * @return whether or not there are any generics contained in {@code type}
   */
  public static boolean hasGenerics(final ParamType type) {
    return !constituentGenerics(type).isEmpty();
  }

  /**
   * Replaces all generics in a schema with concrete schemas defined in {@code resolved}
   *
   * @param schema    the schema which may contain generics
   * @param resolved  the mapping from generics to resolved types
   * @return a schema with the same structure as {@code schema} but with no generics
   *
   * @throws KsqlException if there is a generic in {@code schema} that is not present
   *                       in {@code mapping}
   */
  public static SqlType applyResolved(
      final ParamType schema,
      final Map<GenericType, SqlType> resolved
  ) {
    if (schema instanceof ArrayType) {
      return SqlTypes.array(applyResolved(((ArrayType) schema).element(), resolved));
    }

    if (schema instanceof MapType) {
      final MapType mapType = (MapType) schema;
      final SqlType keyType = applyResolved(mapType.key(), resolved);
      final SqlType valueType = applyResolved(mapType.value(), resolved);
      return SqlTypes.map(keyType, valueType);
    }

    if (schema instanceof StructType) {
      final Builder struct = SqlTypes.struct();
      ((StructType) schema).getSchema().forEach(
          (fieldName, type) -> struct.field(fieldName, applyResolved(type, resolved))
      );
      return struct.build();
    }

    if (schema instanceof GenericType) {
      final SqlType instance = resolved.get(schema);
      if (instance == null) {
        throw new KsqlException("Could not find mapping for generic type: " + schema);
      }
      return instance;
    }

    return SchemaConverters.functionToSqlConverter().toSqlType(schema);
  }

  /**
   * Identifies a mapping from generic type to concrete type based on a {@code schema} and
   * an {@code instance}, where the {@code instance} schema is expected to have no generic
   * types and have the same nested structure as {@code schema}.
   *
   * @param schema    the schema that may contain generics
   * @param instance  a schema with the same structure as {@code schema} but with no generics
   *
   * @return a mapping from generic type to resolved type, an exception is thrown if 
   *         the mapping failed
   */
  public static Map<GenericType, SqlType> reserveGenerics(
      final ParamType schema,
      final SqlArgument instance
  ) {
    final Map<GenericType, SqlType> genericMapping = new HashMap<>();
    final Pair<Boolean, Optional<KsqlException>> result =
        reserveGenerics(schema, instance, genericMapping);
    if (!result.getLeft() && result.getRight().isPresent()) {
      throw result.getRight().get();
    }

    return ImmutableMap.copyOf(genericMapping);
  }

  /**
   * Identifies a mapping from generic type to concrete type based on a {@code schema} and
   * an {@code instance}, where the {@code instance} schema is expected to have no generic
   * types and have the same nested structure as {@code schema}. Adds the mapping to an 
   * existing mapping passed into the function
   *
   * @param schema    the schema that may contain generics
   * @param instance  a schema with the same structure as {@code schema} but with no generics
   * @param reservedGenerics  mapping of generic type to resolved type
   *
   * @return if the mapping succeeded and if it failed, an exception with why it failed
   */
  public static Pair<Boolean, Optional<KsqlException>> reserveGenerics(
      final ParamType schema,
      final SqlArgument instance,
      final Map<GenericType, SqlType> reservedGenerics
  ) {
    final List<Entry<GenericType, SqlType>> genericMapping = new ArrayList<>();
    final boolean success = resolveGenerics(
        genericMapping,
        schema,
        instance
    );

    if (!success) {
      return new Pair<>(false, Optional.of(new KsqlException(
          String.format("Cannot infer generics for %s from %s because "
                  + "they do not have the same schema structure.",
              schema,
              instance))));
    }

    for (final Entry<GenericType, SqlType> entry : genericMapping) {
      final SqlType old = reservedGenerics.putIfAbsent(entry.getKey(), entry.getValue());
      if (old != null && !old.equals(entry.getValue())) {
        return new Pair<>(false, Optional.of(new KsqlException(String.format(
            "Found invalid instance of generic schema when mapping %s to %s. "
                + "Cannot map %s to both %s and %s",
            schema,
            instance,
            entry.getKey(),
            old,
            entry.getValue()))));
      }
    }
    return new Pair<>(true, null);
  }

  /**
   * Identifies a mapping from generic type to concrete type based on a {@code schema} and
   * an {@code instance}, where the {@code instance} schema is expected to have no generic
   * types and have the same nested structure as {@code schema}. Any Generic type mapping
   * identified is added to the list passed in.
   *
   * @param mapping   a list of GenericType to SqlType mappings
   * @param schema    the schema that may contain generics
   * @param instance  a schema with the same structure as {@code schema} but with no generics
   *
   * @return whether we were able to resolve generics in the instance and schema
   */
  // CHECKSTYLE_RULES.OFF: NPathComplexity
  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  private static boolean resolveGenerics(
      final List<Entry<GenericType, SqlType>> mapping,
      final ParamType schema,
      final SqlArgument instance
  ) {
    // CHECKSTYLE_RULES.ON: NPathComplexity
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity

    if (!isGeneric(schema) && !matches(schema, instance)) {
      // cannot identify from type mismatch
      return false;
    } else if (!hasGenerics(schema)) {
      // nothing left to identify
      return true;
    }

    KsqlPreconditions.checkArgument(
        isGeneric(schema) || (matches(schema, instance)),
        "Cannot resolve generics if the schema and instance have differing types: "
            + schema + " vs. " + instance);

    if (schema instanceof LambdaType) {
      final LambdaType lambdaType = (LambdaType) schema;
      final SqlLambda sqlLambda = instance.getSqlLambdaOrThrow();
      if (lambdaType.inputTypes().size() == sqlLambda.getNumInputs()) {
        if (sqlLambda instanceof SqlLambdaResolved) {
          final SqlLambdaResolved sqlLambdaResolved = (SqlLambdaResolved) sqlLambda;
          int i = 0;
          for (final ParamType paramType : lambdaType.inputTypes()) {
            if (!resolveGenerics(
                mapping,
                paramType,
                SqlArgument.of(sqlLambdaResolved.getInputType().get(i))
            )
            ) {
              return false;
            }
            i++;
          }
          return resolveGenerics(
              mapping,
              lambdaType.returnType(),
              SqlArgument.of(sqlLambdaResolved.getReturnType())
          );
        } else {
          return true;
        }
      } else {
        return false;
      }
    }

    final SqlType sqlType = instance.getSqlTypeOrThrow();

    if (isGeneric(schema)) {
      mapping.add(new HashMap.SimpleEntry<>((GenericType) schema, sqlType));
    }

    if (schema instanceof ArrayType) {
      final SqlArray sqlArray = (SqlArray) sqlType;
      return resolveGenerics(
          mapping,
          ((ArrayType) schema).element(),
          SqlArgument.of(sqlArray.getItemType())
      );
    }

    if (schema instanceof MapType) {
      final SqlMap sqlMap = (SqlMap) sqlType;
      final MapType mapType = (MapType) schema;
      return resolveGenerics(mapping, mapType.key(),
          SqlArgument.of(sqlMap.getKeyType()))
          && resolveGenerics(mapping, mapType.value(),
          SqlArgument.of(sqlMap.getValueType()));
    }

    if (schema instanceof StructType) {
      throw new KsqlException("Generic STRUCT is not yet supported");
    }

    return true;
  }

  private static boolean matches(final ParamType schema, final SqlArgument instance) {
    if (instance.getSqlLambda().isPresent()) {
      return schema instanceof LambdaType;
    }
    final ParamType instanceParamType = SchemaConverters
        .sqlToFunctionConverter().toFunctionType(instance.getSqlTypeOrThrow());
    return schema.getClass() == instanceParamType.getClass();
  }
}
