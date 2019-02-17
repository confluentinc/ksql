/*
 * Copyright 2018 Confluent Inc.
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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.internal.KsqlEngineMetrics;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.processing.log.ProcessingLogContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.StatementWithSchema;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public final class KsqlEngineTestUtil {

  private KsqlEngineTestUtil() {
  }

  public static KsqlEngine createKsqlEngine(
      final ServiceContext serviceContext,
      final MutableMetaStore metaStore
  ) {
    return new KsqlEngine(
        serviceContext,
        ProcessingLogContext.create(),
        "test_instance_",
        metaStore,
        KsqlEngineMetrics::new
    );
  }

  public static KsqlEngine createKsqlEngine(
      final ServiceContext serviceContext,
      final MutableMetaStore metaStore,
      final KsqlEngineMetrics engineMetrics
  ) {
    return new KsqlEngine(
        serviceContext,
        ProcessingLogContext.create(),
        "test_instance_",
        metaStore,
        ignored -> engineMetrics
    );
  }

  public static List<QueryMetadata> execute(
      final KsqlEngine engine,
      final String sql,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties
  ) {
    return execute(engine, sql, ksqlConfig, overriddenProperties, Optional.empty());
  }

  /**
   * @param srClient if supplied, then schemas can be inferred from the schema registry.
   */
  public static List<QueryMetadata> execute(
      final KsqlEngine engine,
      final String sql,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties,
      final Optional<SchemaRegistryClient> srClient
  ) {
    final List<ParsedStatement> statements = engine.parse(sql);

    final KsqlExecutionContext sandbox = engine.createSandbox();
    statements.forEach(stmt -> execute(sandbox, stmt, ksqlConfig, overriddenProperties, srClient));

    return statements.stream()
        .map(stmt -> execute(engine, stmt, ksqlConfig, overriddenProperties, srClient))
        .map(ExecuteResult::getQuery)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
  }

  @SuppressWarnings("unchecked")
  private static ExecuteResult execute(
      final KsqlExecutionContext executionContext,
      final ParsedStatement stmt,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties,
      final Optional<SchemaRegistryClient> srClient
  ) {
    final PreparedStatement<?> prepared = executionContext.prepare(stmt);
    final PreparedStatement<?> withSchema = srClient.
        map(client -> StatementWithSchema.forStatement(prepared, client))
        .orElse((PreparedStatement) prepared);
    return executionContext.execute(withSchema, ksqlConfig, overriddenProperties);
  }
}
