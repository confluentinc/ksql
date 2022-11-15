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

package io.confluent.ksql.api.client;

import java.util.Optional;

/**
 * Metadata for a ksqlDB query.
 */
public interface QueryInfo {

  enum QueryType {
    PERSISTENT,
    PUSH
  }

  /**
   * @return the type of this query
   */
  QueryType getQueryType();

  /**
   * Returns the ID of this query, used for control operations such as terminating the query.
   *
   * @return the ID of this query
   */
  String getId();

  /**
   * Returns the ksqlDB statement text corresponding to this query. This text may not be exactly the
   * statement submitted in order to start the query, but submitting this statement will result
   * in exactly this query.
   *
   * @return the ksqlDB statement text
   */
  String getSql();

  /**
   * Returns the name of the sink ksqlDB stream or table that this query writes to, if this query is
   * persistent. If this query is a push query, then the returned optional will be empty.
   *
   * @return the sink ksqlDB stream or table name, if applicable
   */
  Optional<String> getSink();

  /**
   * Returns the name of the Kafka topic that backs the sink ksqlDB stream or table that this query
   * writes to, if this query is persistent. If this query is a push query, then the returned
   * optional will be empty.
   *
   * @return the sink Kafka topic name, if applicable
   */
  Optional<String> getSinkTopic();

}
