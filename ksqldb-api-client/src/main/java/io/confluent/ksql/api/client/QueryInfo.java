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
 *
 */
public interface QueryInfo {

  enum QueryType {
    PERSISTENT,
    PUSH
  }

  QueryType getQueryType();

  /**
   * Query ID, used for control operations such as terminating the query
   */
  String getId();

  String getSql();

  /**
   * Name of sink stream/table, for a persistent query. Else, empty.
   */
  Optional<String> getSink();

  /**
   * Name of sink topic, for a persistent query. Else, empty.
   */
  Optional<String> getSinkTopic();

}
