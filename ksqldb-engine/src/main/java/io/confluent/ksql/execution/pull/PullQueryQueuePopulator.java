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

package io.confluent.ksql.execution.pull;

import java.util.concurrent.CompletableFuture;

public interface PullQueryQueuePopulator {

  /**
   * Runs the pull query asynchronously. When the returned future is complete, the pull query has
   * run to completion and every row has been added to the PullQueryQueue. If there's an error
   * during completion, the future will also complete with an error.
   * @return The future
   */
  CompletableFuture<Void> run();
}
