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

import java.util.List;

public interface ConnectorDescription {
  /**
   * @return class of this connector
   */
  String connectorClass();

  /**
   *
   * @return a list of sources that this connector reads/writes to
   */
  List<String> sources();

  /**
   *
   * @return a list of topics consumed by this connector
   */
  List<String> topics();

  /**
   * @return type of this connector
   */
  ConnectorType type();

  /**
   *
   * @return state of this connector
   */
  String state();
}
