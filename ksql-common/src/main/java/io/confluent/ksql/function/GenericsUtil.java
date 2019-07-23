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
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlPreconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public final class GenericsUtil {

  private static final String PREFIX = "<";
  private static final String SUFFIX = ">";
  private static final Pattern GENERIC_PATTERN = Pattern.compile("<(?<name>.*)>");

  private GenericsUtil() { }

  /**
   * @param typeName  the generic type name (e.g. {@code T})
   * @return a {@link SchemaBuilder} for a generic type with that name
   */
  public static SchemaBuilder generic(final String typeName) {
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
        return constituentGenerics(type.valueSchema());
      case MAP:
        return Sets.union(
            constituentGenerics(type.keySchema()),
            constituentGenerics(type.valueSchema()));
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
   * @param type  the schema
   * @return whether or not there are any generics contained in {@code type}
   */
  public static boolean hasGenerics(final Schema type) {
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
  public static Schema applyResolved(final Schema schema, final Map<Schema, Schema> resolved) {
    switch (schema.type()) {
      case ARRAY:
        return SchemaBuilder
            .array(applyResolved(schema.valueSchema(), resolved))
            .optional()
            .build();
      case MAP:
        return SchemaBuilder
            .map(
                applyResolved(schema.keySchema(), resolved),
                applyResolved(schema.valueSchema(), resolved))
            .optional()
            .build();
      case BYTES:
        if (!isGeneric(schema)) {
          return schema;
        }

        final Schema instance = resolved.get(schema);
        if (instance == null) {
          throw new KsqlException("Could not find mapping for generic type: " + schema);
        }
        return instance;
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
  public static Map<Schema, Schema> resolveGenerics(
      final Schema schema,
      final Schema instance
  ) {
    final List<Entry<Schema, Schema>> genericMapping = new ArrayList<>();
    final boolean success = resolveGenerics(genericMapping, schema, instance);
    if (!success) {
      throw new KsqlException(
          String.format("Cannot infer generics for %s from %s because "
              + "they do not have the same schema structure.",
              schema,
              instance));
    }

    final Map<Schema, Schema> mapping = new HashMap<>();
    for (final Entry<Schema, Schema> entry : genericMapping) {
      final Schema old = mapping.putIfAbsent(entry.getKey(), entry.getValue());
      if (old != null && !old.equals(entry.getValue())) {
        throw new KsqlException(String.format(
            "Found invalid instance of generic schema. Cannot map %s to both %s and %s",
            schema.name(),
            old,
            instance));
      }
    }

    return ImmutableMap.copyOf(mapping);
  }

  private static boolean resolveGenerics(
      final List<Entry<Schema, Schema>> mapping,
      final Schema schema,
      final Schema instance
  ) {
    if (!isGeneric(schema) && instance.type() != schema.type()) {
      // cannot identify from type mismatch
      return false;
    } else if (!hasGenerics(schema)) {
      // nothing left to identify
      return true;
    }

    KsqlPreconditions.checkArgument(
        isGeneric(schema) || (instance.type() == schema.type()),
        "Cannot resolve generics if the schema and instance have differing types: "
            + schema + " vs. " + instance);
    switch (schema.type()) {
      case BYTES:
        mapping.add(new HashMap.SimpleEntry<>(schema, instance));
        return true;
      case ARRAY:
        return resolveGenerics(mapping, schema.valueSchema(), instance.valueSchema());
      case MAP:
        return resolveGenerics(mapping, schema.keySchema(), instance.keySchema())
            && resolveGenerics(mapping, schema.valueSchema(), instance.valueSchema());
      case STRUCT:
        throw new KsqlException("Generic STRUCT is not yet supported");
      default:
        return true;
    }
  }

  /**
   * @param schema    the schema with generics
   * @param instance  a schema without generics
   * @return whether {@code instance} conforms to the structure of {@code schema}
   */
  public static boolean instanceOf(final Schema schema, final Schema instance) {
    final List<Entry<Schema, Schema>> mappings = new ArrayList<>();

    if (!resolveGenerics(mappings, schema, instance)) {
      return false;
    }

    final Map<Schema, Schema> asMap = new HashMap<>();
    for (final Entry<Schema, Schema> entry : mappings) {
      final Schema old = asMap.putIfAbsent(entry.getKey(), entry.getValue());
      if (old != null && !old.equals(entry.getValue())) {
        return false;
      }
    }

    return true;
  }

  /**
   * @param schema the schema to extract the name for
   * @return the name of {@code schema}
   * @throws KsqlException if {@code schema} is not a generic schema
   */
  public static String name(final Schema schema) {
    final Matcher matcher = GENERIC_PATTERN.matcher(schema.name());
    if (matcher.matches()) {
      return matcher.group("name");
    }

    throw new KsqlException("Cannot extract name from non-generic schema: " + schema);
  }
}
