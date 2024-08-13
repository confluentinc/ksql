/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.query;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KeyValueMetadata;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * The queue between the Kafka-streams topology and the client connection.
 *
 * <p>The KS topology writes to the queue from its {@code StreamThread}, while the KSQL server
 * thread that is servicing the client request reads from the queue and writes to the client
 * socket.
 */
public interface BlockingRowQueue {

  /**
   * Sets the limit handler that will be called when any row limit is reached.
   *
   * <p>Replaces any previous handler.
   *
   * @param limitHandler the handler.
   */
  void setLimitHandler(LimitHandler limitHandler);

  /**
   * Sets a handler that will be called when the query completes.
   * Replaces any previous handler.
   */
  void setCompletionHandler(CompletionHandler completionHandler);

  /**
   * Sets the callback that will be called any time a new row is accepted into the queue.
   *
   * @param callback the callback.
   */
  void setQueuedCallback(Runnable callback);

  /**
   * Poll the queue for a single row's key and value, with a timeout.
   *
   * @return the next row
   * @see BlockingQueue#poll(long, TimeUnit)
   */
  KeyValueMetadata<List<?>, GenericRow> poll(long timeout, TimeUnit unit)
      throws InterruptedException;

  /**
   * Poll the queue for a single row's key and value.
   *
   * @return the next row
   * @see BlockingQueue#poll()
   */
  KeyValueMetadata<List<?>, GenericRow> poll();

  /**
   * Drain the queue to the supplied {@code collection}.
   *
   * @param collection the collection to add drained rows to.
   * @see BlockingQueue#drainTo(Collection)
   */
  void drainTo(Collection<? super KeyValueMetadata<List<?>, GenericRow>> collection);

  /**
   * The size of the queue.
   *
   * @see BlockingQueue#size()
   */
  int size();

  /**
   * @return {@code true} if the queue is empty, false otherwise.
   */
  boolean isEmpty();

  /**
   * Close the queue.
   */
  void close();
}
