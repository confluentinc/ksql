/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.ddl.commands;

import java.util.Map;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.SetProperty;

public class SetPropertyCommand implements DdlCommand {
  private final SetProperty statement;

  SetPropertyCommand(final SetProperty statement, final Map<String, Object> properties) {
    this.statement = statement;
    properties.put(statement.getPropertyName(), statement.getPropertyValue());
  }

  @Override
  public DdlCommandResult run(MetaStore metaStore, boolean isValidatePhase) {
    return new DdlCommandResult(true, "property:"
        + statement.getPropertyName()
        + " set to "
        + statement.getPropertyValue()
    );
  }
}
