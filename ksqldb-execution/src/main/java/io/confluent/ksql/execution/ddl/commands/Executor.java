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

package io.confluent.ksql.execution.ddl.commands;

public interface Executor {
  default DdlCommandResult execute(final DdlCommand command) {
    return command.execute(this);
  }

  DdlCommandResult executeCreateStream(CreateStreamCommand createStreamCommand);

  DdlCommandResult executeCreateTable(CreateTableCommand createTableCommand);

  DdlCommandResult executeDropSource(DropSourceCommand dropSource);

  DdlCommandResult executeRegisterType(RegisterTypeCommand registerType);

  DdlCommandResult executeDropType(DropTypeCommand dropType);

  DdlCommandResult executeAlterSource(AlterSourceCommand alterSource);
}
