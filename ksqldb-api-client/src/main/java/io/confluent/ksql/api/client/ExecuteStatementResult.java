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
 * The result of executing a 'CREATE', 'CREATE ... AS * SELECT', 'DROP', 'TERMINATE', or 'INSERT
 * INTO ... AS SELECT' statement on the ksqlDB server.
 */
public interface ExecuteStatementResult {

  /**
   * Returns the ID of a newly started persistent query, if applicable. The return value is empty
   * for all statements other than 'CREATE ... AS * SELECT' and 'INSERT * INTO ... AS SELECT'
   * statements, as only these statements start persistent queries. For statements that start
   * persistent queries, the return value may still be empty if either:
   *
   * <p><ul>
   *   <li> The statement was not executed on the server by the time the server response was sent.
   *   This typically does not happen under normal server operation, but may happen if the ksqlDB
   *   server's command runner thread is stuck, or if the configured value for
   *   {@code ksql.server.command.response.timeout.ms} is too low
   *   <li> The ksqlDB server version is lower than 0.11.0.
   * </ul>
   *
   * @return the query ID, if applicable
   */
  Optional<String> queryId();

}
