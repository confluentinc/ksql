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

import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.util.KsqlConfig;
import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
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
   * @param statementString     The string of the statement to be distributed
   * @param statement           The statement to be distributed
   * @param ksqlConfig          The application-scoped configurations
   * @param overwriteProperties Any command-specific Streams properties to use.
   *
   * @return an asynchronous tracker that can be used to determine the current
   *         state of the command
   */
  QueuedCommandStatus enqueueCommand(
      PreparedStatement<?> statement,
      KsqlConfig ksqlConfig,
      Map<String, Object> overwriteProperties
  );

  /**
   * Polls the Queue for any commands that have been enqueued since the last
   * invocation to this method, blocking until there is data to return. If
   * between invocations to this method, {@link #getRestoreCommands()} is
   * invoked, this command will begin where the results of that call ended.
   *
   * @return a list of commands that have been enqueued since the last call
   * @apiNote this method may block
   */
  List<QueuedCommand> getNewCommands();

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
   * Closes the queue so that no more reads or writes will be accepted.
   */
  @Override
  void close();
}
