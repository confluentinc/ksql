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

package io.confluent.ksql.rest.server.computation;

import io.confluent.ksql.rest.server.ProducerTransactionManager;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Represents a queue of {@link Command}s that must be distributed to all
 * KSQL servers for execution. This queue should be persistent to failure
 * of individual servers, and servers must be able to recover (i.e. build
 * their meta store state) using <i>exclusively</i> data stored within.
 */
public interface CommandQueue extends Closeable {

  /**
   * Enqueues a command onto the command topic. After this method returns,
   * it is guaranteed that the command has been persisted, without regard
   * for the {@link io.confluent.ksql.rest.entity.CommandStatus CommandStatus}.
   *
   * @param statement                   The statement to be distributed
   * @param producerTransactionManager  The transaction manager for enqueueing command
   * @return an asynchronous tracker that can be used to determine the current
   *         state of the command
   */
  QueuedCommandStatus enqueueCommand(
      ConfiguredStatement<?> statement,
      ProducerTransactionManager producerTransactionManager
  );

  /**
   * Polls the Queue for any commands that have been enqueued since the last
   * invocation to this method.
   *
   * <p>The method blocks until either there is data to return or the
   * supplied {@code timeout} expires.
   *
   * <p>If between invocations to this method, {@link #getRestoreCommands()} is
   * invoked, this command will begin where the results of that call ended.
   *
   * @param timeout the max time to wait for new commands.
   * @return a list of commands that have been enqueued since the last call
   * @apiNote this method may block
   */
  List<QueuedCommand> getNewCommands(Duration timeout);

  /**
   * Seeks to the earliest point in history available in the command queue
   * and returns all commands between then and the end of the queue.
   *
   * @return the entire command list history
   * @apiNote this method may block
   */
  List<QueuedCommand> getRestoreCommands();

  /**
   * @param seqNum  the required minimum sequence number to wait for
   * @param timeout throws {@link TimeoutException} if it takes longer that
   *                {@code timeout} to get to {@code seqNum}
   */
  void ensureConsumedPast(long seqNum, Duration timeout)
      throws InterruptedException, TimeoutException;

  /**
   * @return whether or not there are any enqueued commands
   */
  boolean isEmpty();

  /**
   * Cause any blocked {@link #getNewCommands(Duration)} calls to return early.
   *
   * <p>Useful when wanting to {@link #close()} the queue in a timely fashion.
   */
  void wakeup();

  /**
   * Closes the queue so that no more reads or writes will be accepted.
   */
  @Override
  void close();
}
