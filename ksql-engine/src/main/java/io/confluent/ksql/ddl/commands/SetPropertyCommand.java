/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.ddl.commands;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.SetProperty;
import java.util.Map;

public class SetPropertyCommand implements DdlCommand {
  private final SetProperty statement;

  SetPropertyCommand(final SetProperty statement, final Map<String, Object> properties) {
    this.statement = statement;
    properties.put(statement.getPropertyName(), statement.getPropertyValue());
  }

  @Override
  public DdlCommandResult run(final MetaStore metaStore, final boolean isValidatePhase) {
    return new DdlCommandResult(true, "property:"
        + statement.getPropertyName()
        + " set to "
        + statement.getPropertyValue()
    );
  }
}
