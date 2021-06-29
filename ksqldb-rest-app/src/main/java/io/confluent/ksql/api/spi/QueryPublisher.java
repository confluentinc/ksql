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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.KeyValue;
import java.util.List;
import org.reactivestreams.Publisher;

/**
 * Represents a publisher of query results. An instance of this is provided by the back-end for each
 * query that is executed. A subscriber from the API implementation then subscribes to it, then a
 * stream of query results flows from back-end to front-end where they are written to the wire.
 */
public interface QueryPublisher extends Publisher<KeyValue<List<?>, GenericRow>> {

  /**
   * @return List of the names of the columns of the query results
   */
  List<String> getColumnNames();

  /**
   * @return List the types of the columns in the query results
   */
  List<String> getColumnTypes();

  /**
   * Close the publisher
   */
  void close();

  /**
   * @return true if pull query
   */
  boolean isPullQuery();

  /**
   * @return true if scalable push query
   */
  boolean isScalablePushQuery();

  /**
   * The query id
   */
  QueryId queryId();
}
