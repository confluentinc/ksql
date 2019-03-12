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
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import io.confluent.ksql.rest.server.resources.Errors;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

public class DefaultCommandQueueSync implements CommandQueueSync {

  private final CommandQueue commandQueue;
  private final Duration timeout;
  private final Predicate<Class<? extends Statement>> mustSync;

  /**
   * @param commandQueue  the command queue
   * @param mustSync      a predicate describing which statements must wait for previous
   *                      distributed statements to finish before handling (a value of
   *                      {@code true} will require synchronization)
   * @param timeout       the maximum amount of time to wait
   */
  public DefaultCommandQueueSync(
      final CommandQueue commandQueue,
      final Predicate<Class<? extends Statement>> mustSync,
      final Duration timeout
  ) {
    this.commandQueue = Objects.requireNonNull(commandQueue, "commandQueue");
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    this.mustSync = Objects.requireNonNull(mustSync, "mustSync");
  }

  @Override
  public void waitFor(
      final KsqlEntityList previousCommands,
      final Class<? extends Statement> statementClass) {
    if (mustSync.test(statementClass)) {
      final ArrayList<KsqlEntity> reversed = new ArrayList<>(previousCommands);
      Collections.reverse(reversed);

      reversed.stream()
          .filter(e -> e instanceof CommandStatusEntity)
          .map(CommandStatusEntity.class::cast)
          .map(CommandStatusEntity::getCommandSequenceNumber)
          .findFirst()
          .ifPresent(seqNum -> {
            try {
              commandQueue.ensureConsumedPast(seqNum, timeout);
            } catch (final InterruptedException e) {
              throw new KsqlRestException(Errors.serverShuttingDown());
            } catch (final TimeoutException e) {
              throw new KsqlRestException(Errors.commandQueueCatchUpTimeout(seqNum));
            }
          });
    }
  }
}
