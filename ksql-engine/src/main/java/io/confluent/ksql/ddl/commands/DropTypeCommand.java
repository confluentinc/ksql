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
import java.util.Objects;

public class DropTypeCommand implements DdlCommand {

  private final String typeName;

  public DropTypeCommand(final String typeName) {
    this.typeName = Objects.requireNonNull(typeName, "typeName");
  }

  @Override
  public DdlCommandResult run(final MutableMetaStore metaStore) {
    final boolean wasDeleted = metaStore.deleteType(typeName);
    return wasDeleted
        ? new DdlCommandResult(true, "Dropped type '" + typeName + "'")
        : new DdlCommandResult(true, "Type '" + typeName + "' does not exist");
  }
}
