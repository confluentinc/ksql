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
import io.confluent.ksql.util.KsqlException;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

class TypeRegistryImpl implements TypeRegistry {

  private final Map<String, SqlType> typeRegistry = new ConcurrentHashMap<>();

  @Override
  public void registerType(final String alias, final SqlType type) {
    final SqlType oldValue = typeRegistry.putIfAbsent(alias.toUpperCase(), type);
    if (oldValue != null) {
      throw new KsqlException(
          "Cannot register alias " + alias + " since it is already registered with type: " + type
      );
    }
  }

  @Override
  public boolean deleteType(final String alias) {
    return typeRegistry.remove(alias.toUpperCase()) != null;
  }

  @Override
  public Optional<SqlType> resolveType(final String alias) {
    return Optional.ofNullable(typeRegistry.get(alias.toUpperCase()));
  }

  @Override
  public Iterator<TypeAlias> types() {
    return typeRegistry
        .entrySet()
        .stream()
        .map(kv -> new TypeAlias(kv.getKey(), kv.getValue())).iterator();
  }

}
