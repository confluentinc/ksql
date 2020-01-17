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

package io.confluent.ksql.api.spi;

import io.vertx.core.json.JsonArray;
import org.reactivestreams.Publisher;

/**
 * Represents a publisher of query results. An instance of this is provided by the back-end for each
 * query that is executed. A subscriber from the API implementation then subscribes to it, then a
 * stream of query results flows from back-end to front-end where they are written to the wire.
 */
public interface QueryPublisher extends Publisher<JsonArray> {

  /**
   * @return Array representing the names of the columns of the query results
   */
  JsonArray getColumnNames();

  /**
   * @return Array representing the types of the columns in the query results
   */
  JsonArray getColumnTypes();

  /**
   * @return For a pull query must return the number of rows in the results otherwise -1
   */
  int getRowCount();

}
