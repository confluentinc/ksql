/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.api.client;

public interface ConnectorInfo {

  /**
   * @return name of this connector
   */
  String name();

  /**
   * @return type of this connector
   */
  ConnectorType type();

  /**
   * @return class of this connector
   */
  String className();

  /**
   * @return state of this connector. The possible states are
   *     "UNASSIGNED", "RUNNING", "FAILED" or "PAUSED".
   */
  String state();
}
