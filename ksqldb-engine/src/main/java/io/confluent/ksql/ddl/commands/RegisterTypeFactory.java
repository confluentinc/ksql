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

package io.confluent.ksql.ddl.commands;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.execution.ddl.commands.RegisterTypeCommand;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.RegisterType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;

public final class RegisterTypeFactory {
  private final MetaStore metaStore;

  RegisterTypeFactory(final MetaStore metaStore) {
    this.metaStore = requireNonNull(metaStore, "metaStore");
  }

  public RegisterTypeCommand create(final RegisterType statement) {
    final String name = statement.getName();
    final boolean ifNotExists = statement.getIfNotExists();
    final SqlType type = statement.getType().getSqlType();

    if (!ifNotExists && metaStore.resolveType(name).isPresent()) {
      throw new KsqlException(
          "Cannot register custom type '" + name + "' "
              + "since it is already registered with type: " + metaStore.resolveType(name).get()
      );
    }

    return new RegisterTypeCommand(type, name);
  }
}
