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

package io.confluent.ksql.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public final class GenericsUtil {

  private static final String PREFIX = "<";
  private static final String SUFFIX = ">";

  private GenericsUtil() { }

  /**
   * @param typeName  the generic type name (e.g. {@code T})
   * @return a {@link SchemaBuilder} for a generic type with that name
   */
  public static SchemaBuilder generic(final String typeName) {
    KsqlPreconditions.checkArgument(
        typeName.chars().allMatch(Character::isLetter),
        "Generic types must be all letters");

    return SchemaBuilder.bytes().optional().name(PREFIX + typeName + SUFFIX);
  }

  /**
   * @param typeName  the generic type name (e.g. {@code T})
   * @return a {@link SchemaBuilder} for a generic array with generic element type
   */
  public static SchemaBuilder array(final String typeName) {
    return SchemaBuilder.array(generic(typeName).build()).optional();
  }

  /**
   * @param keyType       the type for the map key
   * @param valueTypeName the generic type name (e.g. {@code T}) for the map value
   * @return a {@link SchemaBuilder} for a map with {@code keyType} keys and generic value type
   */
  public static SchemaBuilder map(final Schema keyType, final String valueTypeName) {
    return SchemaBuilder.map(keyType, generic(valueTypeName).build()).optional();
  }

  /**
   * @param type  the type
   * @return whether or not {@code type} is a generic type
   * @apiNote container generics (e.g. {@code ARRAY<T>}) will return {@code false},
   *          use {@link #constituentGenerics(Schema)}
   */
  public static boolean isGeneric(final Schema type) {
    return type.name() != null
        && type.name().startsWith(PREFIX)
        && type.name().endsWith(SUFFIX);
  }

  /**
   * @param type  the type
   * @return all generics contained within the type, for example: {@code Map<K, V>} would return
   *         a set containing {@code {<K>, <V>}}
   */
  public static Set<Schema> constituentGenerics(final Schema type) {
    switch (type.type()) {
      case ARRAY:
      case MAP:
        return constituentGenerics(type.valueSchema());
      case STRUCT:
        return type.fields().stream()
            .map(Field::schema)
            .map(GenericsUtil::constituentGenerics)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());
      case BYTES:
        if (isGeneric(type)) {
          return ImmutableSet.of(type);
        }
        return ImmutableSet.of();
      default:
        return ImmutableSet.of();
    }
  }

  /**
   * Replaces all generics in a schema with concrete schemas defined in {@code mapping}
   *
   * @param schema  the schema which may contain generics
   * @param mapping the mapping from generics to resolved types
   * @return a schema with the same structure as {@code schema} but with no generics
   *
   * @throws KsqlException if there is a generic in {@code schema} that is not present
   *                       in {@code mapping}
   */
  public static Schema resolve(final Schema schema, final Map<Schema, Schema> mapping) {
    switch (schema.type()) {
      case ARRAY:
        return SchemaBuilder
            .array(resolve(schema.valueSchema(), mapping))
            .optional()
            .build();
      case MAP:
        return SchemaBuilder
            .map(resolve(schema.keySchema(), mapping), resolve(schema.valueSchema(), mapping))
            .optional()
            .build();
      case BYTES:
        if (!isGeneric(schema)) {
          return schema;
        }

        final Schema resolved = mapping.get(schema);
        if (resolved == null) {
          throw new KsqlException("Could not find mapping for generic type: " + schema);
        }
        return resolved;
      default:
        return schema;
    }
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
  public static Map<Schema, Schema> identifyGenerics(
      final Schema schema,
      final Schema instance
  ) {
    final Map<Schema, Schema> genericMapping = new HashMap<>();
    identifyGenerics(genericMapping, schema, instance);
    return ImmutableMap.copyOf(genericMapping);
  }

  public static void identifyGenerics(
      final Map<Schema, Schema> mapping,
      final Schema schema,
      final Schema instance
  ) {
    KsqlPreconditions.checkArgument(
        isGeneric(schema) || (instance.type() == schema.type()),
        "Cannot resolve generics if the schema and instance have differing types: "
            + schema + " vs. " + instance);
    switch (schema.type()) {
      case BYTES:
        if (isGeneric(schema)) {
          final Schema old = mapping.putIfAbsent(schema, instance);
          if (old != null && !old.equals(instance)) {
            throw new KsqlException(String.format(
                "Found invalid instance of generic schema. Cannot map %s to both %s and %s",
                schema.name(),
                old,
                instance));
          }
        }
        break;
      case ARRAY:
        identifyGenerics(mapping, schema.valueSchema(), instance.valueSchema());
        break;
      case MAP:
        identifyGenerics(mapping, schema.keySchema(), instance.keySchema());
        identifyGenerics(mapping, schema.valueSchema(), instance.valueSchema());
        break;
      case STRUCT:
        throw new KsqlException("Generic STRUCT is not yet supported");
      default:
        break;
    }
  }

  public static String name(final Schema schema) {
    return schema.name().substring(1, schema.name().length() - 1);
  }
}
