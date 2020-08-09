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

import io.confluent.ksql.execution.ddl.commands.DropTypeCommand;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.DropType;
import io.confluent.ksql.util.KsqlException;

import java.util.Objects;

public class DropTypeFactory {
  private final MetaStore metaStore;

  DropTypeFactory(final MetaStore metaStore) {
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
  }

  public DropTypeCommand create(final DropType statement) {
    final String typeName = statement.getTypeName();
    final boolean ifExists = statement.getIfExists();

    if (!ifExists && !metaStore.resolveType(typeName).isPresent()) {
      throw new KsqlException("Type " + typeName + " does not exist.");
    }

    return new DropTypeCommand(typeName);
  }
}
