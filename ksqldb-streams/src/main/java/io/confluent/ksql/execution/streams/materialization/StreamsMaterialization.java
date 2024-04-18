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

package io.confluent.ksql.execution.streams.materialization;

import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Optional;

public interface StreamsMaterialization {

  /**
   * @return The schema of the materialization
   */
  LogicalSchema schema();

  /**
   * @return service for locating which node holds specific data.
   */
  Locator locator();

  /**
   * @return the window type of the materialization.
   */
  Optional<WindowType> windowType();

  /**
   * @return access to the materialized non-windowed table.
   */
  StreamsMaterializedTable nonWindowed();

  /**
   * @return access to the materialized windowed table.
   */
  StreamsMaterializedWindowedTable windowed();
}
