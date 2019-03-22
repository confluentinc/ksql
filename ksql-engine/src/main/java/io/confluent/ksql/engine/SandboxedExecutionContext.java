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

package io.confluent.ksql.engine;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * An execution context that can execute statements without changing the core engine's state
 * or the state of external services.
 */
final class SandboxedExecutionContext implements KsqlExecutionContext {

  private final EngineContext engineContext;

  SandboxedExecutionContext(final EngineContext sourceContext) {
    this.engineContext = sourceContext.createSandbox();
  }

  @Override
  public MetaStore getMetaStore() {
    return engineContext.getMetaStore();
  }

  @Override
  public KsqlExecutionContext createSandbox() {
    return new SandboxedExecutionContext(engineContext);
  }

  @Override
  public Optional<PersistentQueryMetadata> getPersistentQuery(final QueryId queryId) {
    return engineContext.getPersistentQuery(queryId);
  }

  @Override
  public List<PersistentQueryMetadata> getPersistentQueries() {
    return ImmutableList.copyOf(engineContext.getPersistentQueries().values());
  }

  @Override
  public List<ParsedStatement> parse(final String sql) {
    return engineContext.parse(sql);
  }

  @Override
  public PreparedStatement<?> prepare(final ParsedStatement stmt) {
    return engineContext.prepare(stmt);
  }

  @Override
  public ExecuteResult execute(
      final PreparedStatement<?> statement,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> overriddenProperties
  ) {
    final EngineExecutor executor = EngineExecutor
        .create(engineContext, ksqlConfig, overriddenProperties);

    return executor.execute(statement);
  }
}
