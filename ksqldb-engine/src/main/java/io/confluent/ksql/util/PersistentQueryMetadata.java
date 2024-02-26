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

package io.confluent.ksql.util;

import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.scalablepush.ScalablePushRegistry;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryError;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.query.QuerySchemas;
import java.util.Optional;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

public interface PersistentQueryMetadata extends QueryMetadata {

  Optional<DataSource.DataSourceType> getDataSourceType();

  Optional<KsqlTopic> getResultTopic();

  Optional<SourceName> getSinkName();

  QuerySchemas getQuerySchemas();

  PhysicalSchema getPhysicalSchema();

  ExecutionStep<?> getPhysicalPlan();

  Optional<DataSource> getSink();

  KsqlConstants.PersistentQueryType getPersistentQueryType();

  ProcessingLogger getProcessingLogger();

  Optional<Materialization> getMaterialization(
      QueryId queryId,
      QueryContext.Stacker contextStacker
  );

  void stop();

  void stop(boolean isCreateOrReplace);

  void register();

  StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse uncaughtHandler(
      Throwable error
  );

  Optional<ScalablePushRegistry> getScalablePushRegistry();

  final class QueryListenerWrapper implements Listener {
    private final Listener listener;
    private final Optional<ScalablePushRegistry> scalablePushRegistry;

    protected QueryListenerWrapper(final Listener listener,
                                   final Optional<ScalablePushRegistry> scalablePushRegistry) {
      this.listener = listener;
      this.scalablePushRegistry = scalablePushRegistry;
    }

    @Override
    public void onError(final QueryMetadata queryMetadata, final QueryError error) {
      this.listener.onError(queryMetadata, error);
      scalablePushRegistry.ifPresent(ScalablePushRegistry::onError);
    }

    @Override
    public void onStateChange(final QueryMetadata query, final State old, final State newState) {
      this.listener.onStateChange(query, old, newState);
    }

    @Override
    public void onClose(final QueryMetadata queryMetadata) {
      this.listener.onClose(queryMetadata);
      scalablePushRegistry.ifPresent(ScalablePushRegistry::cleanup);
    }
  }
}
