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
 * An {@code Executable} is a lifecycle interface that does not conflict with
 * {@link org.eclipse.jetty.util.component.LifeCycle} so that it can be used
 * to specify additional operations during start/stop/join.
 */
public interface Executable {

  /**
   * Starts the executable asynchronously.
   */
  default void startAsync() throws Exception {}

  /**
   * Triggers a shutdown asynchronously, in order to ensure that the shutdown
   * has finished use {@link #awaitTerminated()}
   */
  default void triggerShutdown() throws Exception {}

  /**
   * Awaits the {@link #triggerShutdown()} to finish. This is a blocking
   * operation.
   */
  default void awaitTerminated() throws InterruptedException {}
}
