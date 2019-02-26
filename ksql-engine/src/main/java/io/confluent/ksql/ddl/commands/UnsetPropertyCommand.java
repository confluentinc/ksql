/*
 * Copyright 2018 Confluent Inc.
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

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.UnsetProperty;
import java.util.Map;

public class UnsetPropertyCommand implements DdlCommand {

  private final UnsetProperty unsetProperty;

  public UnsetPropertyCommand(final UnsetProperty unsetProperty,
      final Map<String, Object> properties) {
    this.unsetProperty = unsetProperty;
    properties.remove(unsetProperty.getPropertyName());
  }

  @Override
  public DdlCommandResult run(final MetaStore metaStore, final boolean isValidatePhase) {
    return new DdlCommandResult(true, "property:"
        + unsetProperty.getPropertyName()
        + " was removed. "
    );
  }
}
