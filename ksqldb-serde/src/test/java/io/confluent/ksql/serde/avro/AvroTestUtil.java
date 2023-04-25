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

package io.confluent.ksql.serde.avro;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.avro.generic.GenericContainer;

final class AvroTestUtil {

  private AvroTestUtil() {
  }

  /**
   * Avro friendly map impl.
   *
   * <p>Avro does not supply a `GenericMap` to go with its `GenericArray`. So here's one to use.
   *
   * @param <K> the key type.
   * @param <V> the value type.
   */
  public static class GenericMap<K, V>
      extends AbstractMap<K, V>
      implements GenericContainer {

    private final org.apache.avro.Schema schema;
    private final Map<K, V> map;

    GenericMap(final org.apache.avro.Schema schema, final Map<K, V> map) {
      this.schema = Objects.requireNonNull(schema, "schema");
      this.map = Objects.requireNonNull(map, "map");
    }

    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP")
    public org.apache.avro.Schema getSchema() {
      return schema;
    }

    @Nonnull
    @Override
    public Set<Entry<K, V>> entrySet() {
      return map.entrySet();
    }
  }

  /**
   * Build a Connect map entry schema for a given {@code valueSchema} and name.
   *
   * <p>Connect persists its {@link org.apache.kafka.connect.data.Schema.Type#MAP} type, where the
   * key is <i>optional</i>, as an array of key-value entries.
   *
   * @param name the name of the map. Used to avoid type name collisions should a schema have more
   *             than one map type.
   * @param valueSchema the schema of the value type of the map.
   *
   * @return the schema of the entry, i.e. the element schema for the array.
   */
  static org.apache.avro.Schema connectOptionalKeyMapEntrySchema(
      final String name,
      final org.apache.avro.Schema valueSchema
  ) {
    return org.apache.avro.SchemaBuilder.record(name)
        .namespace("io.confluent.ksql.avro_schemas")
        .prop("connect.internal.type", "MapEntry")
        .fields()
        .optionalString("key")
        .name("value")
        .type().unionOf().nullType().and().type(valueSchema).endUnion()
        .nullDefault()
        .endRecord();
  }

  /**
   * Build an Avro schema to hold a Connect map with optional keys.
   *
   * <p>Connect persists its {@link org.apache.kafka.connect.data.Schema.Type#MAP} type, where the
   *    * key is <i>optional</i>, as an array of key-value entries.
   *
   * @param entrySchema the schema of the entry, built by {@link #connectOptionalKeyMapEntrySchema}.
   * @return the schema of the array.
   */
  static org.apache.avro.Schema connectOptionalKeyMapSchema(
      final org.apache.avro.Schema entrySchema
  ) {
    return org.apache.avro.SchemaBuilder
        .array()
        .items(entrySchema);
  }
}
