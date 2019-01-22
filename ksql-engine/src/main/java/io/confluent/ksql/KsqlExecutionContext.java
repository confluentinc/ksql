/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Map;
import java.util.Optional;

/**
 * The context in which statements can be executed.
 */
public interface KsqlExecutionContext {

  /**
   * @return create a sandboxed execution context as a copy of this context.
   */
  KsqlExecutionContext createSandbox();

  /**
   * @return read-only access to the context's {@link MetaStore}.
   */
  MetaStore getMetaStore();

  /**
   * Retrieve the details of a persistent query.
   *
   * @param queryId the id of the query to retrieve.
   * @return the query's details or else {@code Optional.empty()} if no found.
   */
  Optional<PersistentQueryMetadata> getPersistentQuery(QueryId queryId);

  /**
   * Execute a statement within the context scope.
   *
   * @param statement the statement to execute
   * @param ksqlConfig the configuration to use.
   * @param overriddenProperties any overridden properties
   * @return query metadata if the statement contained a query, {@link Optional#empty()} otherwise
   */
  Optional<QueryMetadata> execute(
      PreparedStatement<?> statement,
      KsqlConfig ksqlConfig,
      Map<String, Object> overriddenProperties);
}
