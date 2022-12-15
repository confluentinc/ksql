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

package io.confluent.ksql.rest.server;

/**
 * An {@code Executable} is a lifecycle interface
 */
public interface Executable {

  /**
   * Starts the executable asynchronously. Guaranteed to be called before shutdown.
   */
  default void startAsync() throws Exception {

  }

  /**
   * Called to notify threads awaiting termination (see #awaitTerminated)
   * that it's time to shutdown.
   */
  default void notifyTerminated() {

  }

  /**
   * Shutdown the service.
   */
  default void shutdown() throws Exception {

  }

  /**
   * Awaits the {@link #notifyTerminated()} notification. This is a blocking
   * operation. Guaranteed to be called before shutdown.
   */
  default void awaitTerminated() throws InterruptedException {

  }
}
