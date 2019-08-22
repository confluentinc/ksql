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
import java.util.Objects;
import java.util.Optional;

/**
 * {@code TypeRegistry} maintains a mapping from custom schema aliases
 * to more complicated schemas.
 */
public interface TypeRegistry {

  /**
   * Registers an alias with a specified schema
   *
   * @param alias   the name, must be unique
   * @param type    the schema to associate it with
   */
  void registerType(String alias, SqlType type);

  /**
   * @param alias the previously registered alias
   * @return whether or not a type was dropped
   */
  boolean deleteType(String alias);

  /**
   * Resolves an alias to a previously registered type
   *
   * @param alias the alias name
   * @return the type it was registered with, or {@link Optional#empty()} if
   *         there was no alias with this name registered
   */
  Optional<SqlType> resolveType(String alias);

  /**
   * @return an iterable of all types registered in this registry
   */
  Iterator<TypeAlias> types();

  class TypeAlias {
    private final String alias;
    private final SqlType type;

    public TypeAlias(final String alias, final SqlType type) {
      this.alias = Objects.requireNonNull(alias, "alias");
      this.type = Objects.requireNonNull(type, "type");
    }

    public String getAlias() {
      return alias;
    }

    public SqlType getType() {
      return type;
    }
  }

}
