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
import io.confluent.ksql.function.types.BooleanType;
import io.confluent.ksql.function.types.DecimalType;
import io.confluent.ksql.function.types.DoubleType;
import io.confluent.ksql.function.types.GenericType;
import io.confluent.ksql.function.types.IntegerType;
import io.confluent.ksql.function.types.LongType;
import io.confluent.ksql.function.types.MapType;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.types.StringType;
import io.confluent.ksql.function.types.StructType;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlStruct.Builder;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlPreconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
   * @return a mapping from generic type to resolved type
   */
  public static Map<GenericType, SqlType> resolveGenerics(
      final ParamType schema,
      final SqlType instance
  ) {
    final List<Entry<GenericType, SqlType>> genericMapping = new ArrayList<>();
    final boolean success = resolveGenerics(genericMapping, schema, instance);
    if (!success) {
      throw new KsqlException(
          String.format("Cannot infer generics for %s from %s because "
              + "they do not have the same schema structure.",
              schema,
              instance));
    }

    final Map<GenericType, SqlType> mapping = new HashMap<>();
    for (final Entry<GenericType, SqlType> entry : genericMapping) {
      final SqlType old = mapping.putIfAbsent(entry.getKey(), entry.getValue());
      if (old != null && !old.equals(entry.getValue())) {
        throw new KsqlException(String.format(
            "Found invalid instance of generic schema. Cannot map %s to both %s and %s",
            schema,
            old,
            instance));
      }
    }

    return ImmutableMap.copyOf(mapping);
  }

  private static boolean resolveGenerics(
      final List<Entry<GenericType, SqlType>> mapping,
      final ParamType schema,
      final SqlType instance
  ) {
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

    if (isGeneric(schema)) {
      mapping.add(new HashMap.SimpleEntry<>((GenericType) schema, instance));
    }

    if (schema instanceof ArrayType) {
      return resolveGenerics(
          mapping, ((ArrayType) schema).element(), ((SqlArray) instance).getItemType());
    }

    if (schema instanceof MapType) {
      final SqlMap sqlMap = (SqlMap) instance;
      final MapType mapType = (MapType) schema;
      return resolveGenerics(mapping, mapType.key(), sqlMap.getKeyType())
          && resolveGenerics(mapping, mapType.value(), sqlMap.getValueType());
    }

    if (schema instanceof StructType) {
      throw new KsqlException("Generic STRUCT is not yet supported");
    }

    return true;
  }

  private static boolean matches(final ParamType schema, final SqlType instance) {
    switch (instance.baseType()) {
      case BOOLEAN: return schema instanceof BooleanType;
      case INTEGER: return schema instanceof IntegerType;
      case BIGINT:  return schema instanceof LongType;
      case DECIMAL: return schema instanceof DecimalType;
      case DOUBLE:  return schema instanceof DoubleType;
      case STRING:  return schema instanceof StringType;
      case ARRAY:   return schema instanceof ArrayType;
      case MAP:     return schema instanceof MapType;
      case STRUCT:  return schema instanceof StructType;
      default:      return false;
    }
  }

  /**
   * @param schema    the schema with generics
   * @param instance  a schema without generics
   * @return whether {@code instance} conforms to the structure of {@code schema}
   */
  public static boolean instanceOf(final ParamType schema, final SqlType instance) {
    final List<Entry<GenericType, SqlType>> mappings = new ArrayList<>();

    if (!resolveGenerics(mappings, schema, instance)) {
      return false;
    }

    final Map<ParamType, SqlType> asMap = new HashMap<>();
    for (final Entry<GenericType, SqlType> entry : mappings) {
      final SqlType old = asMap.putIfAbsent(entry.getKey(), entry.getValue());
      if (old != null && !old.equals(entry.getValue())) {
        return false;
      }
    }

    return true;
  }
}
