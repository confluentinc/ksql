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

import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.avro.generic.GenericContainer;

final class AvroTestUtil {

  private AvroTestUtil() {
  }

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
    public org.apache.avro.Schema getSchema() {
      return schema;
    }

    @Nonnull
    @Override
    public Set<Entry<K, V>> entrySet() {
      return map.entrySet();
    }
  }
}
