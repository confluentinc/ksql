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

package io.confluent.ksql.parser.tree;

import java.util.Optional;

/**
 * Indicates the statement contains a {@link Query}.
 */
public interface QueryContainer {

  /**
   * Get the contained query.
   *
   * @return the query
   */
  Query getQuery();

  /**
   * Get the information about the sink the query will output to.
   *
   * @return the sink info.
   */
  Sink getSink();

  /**
   * Return an optional query ID if specified by the user. This query ID is used only by
   * "INSERT INTO WITH (QUERY_ID='value')" statements.
   *
   * @return the optional query ID
   */
  Optional<String> getQueryId();
}
