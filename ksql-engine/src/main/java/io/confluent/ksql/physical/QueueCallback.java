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

package io.confluent.ksql.physical;

/**
 * A callback called before and after things are queued.
 */
public interface QueueCallback {

  /**
   * Called to determine is an output row should be queued for output.
   *
   * @return {@code true} if it should be sent, {@code false} otherwise.
   */
  boolean shouldQueue();

  /**
   * Called once a row has been queued for output.
   */
  void onQueued();
}
