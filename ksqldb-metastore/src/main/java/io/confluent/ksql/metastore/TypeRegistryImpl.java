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

package io.confluent.ksql.metastore;

import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class TypeRegistryImpl implements TypeRegistry {

  private final Map<String, SqlType> typeRegistry = new ConcurrentHashMap<>();

  @Override
  public boolean registerType(final String name, final SqlType type) {
    return typeRegistry.putIfAbsent(name.toUpperCase(), type) == null;
  }

  @Override
  public boolean deleteType(final String name) {
    return typeRegistry.remove(name.toUpperCase()) != null;
  }

  @Override
  public Optional<SqlType> resolveType(final String name) {
    return Optional.ofNullable(typeRegistry.get(name.toUpperCase()));
  }

  @Override
  public Iterator<CustomType> types() {
    return typeRegistry
        .entrySet()
        .stream()
        .map(kv -> new CustomType(kv.getKey(), kv.getValue())).iterator();
  }

}
