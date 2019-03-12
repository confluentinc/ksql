/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.server.execution;

import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.entity.KsqlEntityList;

/**
 * {@code CommandQueueSync} provides encapsulated functionality to
 * wait until a list of commands has been executed remotely.
 */
public interface CommandQueueSync {

  /**
   * Waits for the previous commands to complete.
   *
   * @param previousCommands the list of previously executed commands
   * @param statementClass the type of statement that is being waited on
   * @apiNote this is a blocking operation
   */
  void waitFor(KsqlEntityList previousCommands, Class<? extends Statement> statementClass);

}
