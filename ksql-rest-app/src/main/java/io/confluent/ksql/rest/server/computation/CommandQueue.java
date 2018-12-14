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

package io.confluent.ksql.rest.server.computation;

import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.KsqlConfig;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public interface CommandQueue extends Closeable {
  QueuedCommandStatus enqueueCommand(
      String statementString,
      Statement statement,
      KsqlConfig ksqlConfig,
      Map<String, Object> overwriteProperties
  );

  List<QueuedCommand> getNewCommands();

  List<QueuedCommand> getRestoreCommands();

  void ensureConsumedPast(long seqNum, Duration timeout)
      throws InterruptedException, TimeoutException;
}
