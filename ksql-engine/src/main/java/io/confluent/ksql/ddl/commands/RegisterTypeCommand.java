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

package io.confluent.ksql.ddl.commands;

import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.tree.RegisterType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.Objects;

public class RegisterTypeCommand implements DdlCommand {

  private final RegisterType statement;

  public RegisterTypeCommand(final RegisterType statement) {
    this.statement = Objects.requireNonNull(statement, "statement");
  }

  @Override
  public DdlCommandResult run(final MutableMetaStore metaStore) {
    final SqlType sqlType = statement.getType().getSqlType();

    metaStore.registerType(statement.getName(), sqlType);

    return new DdlCommandResult(
        true,
        "Registered custom type with name '" + statement.getName() + "' and SQL type " + sqlType
    );
  }
}
