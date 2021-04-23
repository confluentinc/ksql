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
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.execution.streams.materialization.MaterializationProvider;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.physical.scalablepush.ScalablePushRegistry;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.query.QuerySchemas;
import java.util.Optional;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

public interface PersistentQueryMetadata extends QueryMetadata {

  DataSource.DataSourceType getDataSourceType();

  KsqlTopic getResultTopic();

  SourceName getSinkName();

  QuerySchemas getQuerySchemas();

  PhysicalSchema getPhysicalSchema();

  ExecutionStep<?> getPhysicalPlan();

  DataSource getSink();

  ProcessingLogger getProcessingLogger();

  Optional<Materialization> getMaterialization(
      QueryId queryId,
      QueryContext.Stacker contextStacker
  );

  void restart();

  void stop();

  StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse uncaughtHandler(
      Throwable error
  );

  Optional<MaterializationProvider>  getMaterializationProvider();

  Optional<ScalablePushRegistry> getScalablePushRegistry();
}
