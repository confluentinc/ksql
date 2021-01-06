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

package io.confluent.ksql.structured;

import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.SourceStep;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.plan.TableSource;
import io.confluent.ksql.execution.plan.WindowedStreamSource;
import io.confluent.ksql.execution.plan.WindowedTableSource;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.execution.streams.StepSchemaResolver;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.planner.plan.PlanBuildContext;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.WindowInfo;

/**
 * Factory class used to create stream and table sources
 */
public final class SchemaKSourceFactory {

  private SchemaKSourceFactory() {
  }

  public static SchemaKStream<?> buildSource(
      final PlanBuildContext buildContext,
      final DataSource dataSource,
      final QueryContext.Stacker contextStacker
  ) {
    final boolean windowed = dataSource.getKsqlTopic().getKeyFormat().isWindowed();
    switch (dataSource.getDataSourceType()) {
      case KSTREAM:
        return windowed
            ? buildWindowedStream(
            buildContext,
            dataSource,
            contextStacker
        ) : buildStream(
            buildContext,
            dataSource,
            contextStacker
        );

      case KTABLE:
        return windowed
            ? buildWindowedTable(
            buildContext,
            dataSource,
            contextStacker
        ) : buildTable(
            buildContext,
            dataSource,
            contextStacker
        );

      default:
        throw new UnsupportedOperationException("Source type:" + dataSource.getDataSourceType());
    }
  }

  private static SchemaKStream<?> buildWindowedStream(
      final PlanBuildContext buildContext,
      final DataSource dataSource,
      final Stacker contextStacker
  ) {
    final WindowInfo windowInfo = dataSource.getKsqlTopic().getKeyFormat().getWindowInfo()
        .orElseThrow(IllegalArgumentException::new);

    final WindowedStreamSource step = ExecutionStepFactory.streamSourceWindowed(
        contextStacker,
        dataSource.getSchema(),
        dataSource.getKafkaTopicName(),
        Formats.from(dataSource.getKsqlTopic()),
        windowInfo,
        dataSource.getTimestampColumn()
    );

    return schemaKStream(
        buildContext,
        resolveSchema(buildContext, step, dataSource),
        dataSource.getKsqlTopic().getKeyFormat(),
        step
    );
  }

  private static SchemaKStream<?> buildStream(
      final PlanBuildContext buildContext,
      final DataSource dataSource,
      final Stacker contextStacker
  ) {
    if (dataSource.getKsqlTopic().getKeyFormat().getWindowInfo().isPresent()) {
      throw new IllegalArgumentException("windowed");
    }

    final StreamSource step = ExecutionStepFactory.streamSource(
        contextStacker,
        dataSource.getSchema(),
        dataSource.getKafkaTopicName(),
        Formats.from(dataSource.getKsqlTopic()),
        dataSource.getTimestampColumn()
    );

    return schemaKStream(
        buildContext,
        resolveSchema(buildContext, step, dataSource),
        dataSource.getKsqlTopic().getKeyFormat(),
        step
    );
  }

  private static SchemaKTable<?> buildWindowedTable(
      final PlanBuildContext buildContext,
      final DataSource dataSource,
      final Stacker contextStacker
  ) {
    final WindowInfo windowInfo = dataSource.getKsqlTopic().getKeyFormat().getWindowInfo()
        .orElseThrow(IllegalArgumentException::new);

    final WindowedTableSource step = ExecutionStepFactory.tableSourceWindowed(
        contextStacker,
        dataSource.getSchema(),
        dataSource.getKafkaTopicName(),
        Formats.from(dataSource.getKsqlTopic()),
        windowInfo,
        dataSource.getTimestampColumn()
    );

    return schemaKTable(
        buildContext,
        resolveSchema(buildContext, step, dataSource),
        dataSource.getKsqlTopic().getKeyFormat(),
        step
    );
  }

  private static SchemaKTable<?> buildTable(
      final PlanBuildContext buildContext,
      final DataSource dataSource,
      final Stacker contextStacker
  ) {
    if (dataSource.getKsqlTopic().getKeyFormat().getWindowInfo().isPresent()) {
      throw new IllegalArgumentException("windowed");
    }

    final TableSource step = ExecutionStepFactory.tableSource(
        contextStacker,
        dataSource.getSchema(),
        dataSource.getKafkaTopicName(),
        Formats.from(dataSource.getKsqlTopic()),
        dataSource.getTimestampColumn()
    );

    return schemaKTable(
        buildContext,
        resolveSchema(buildContext, step, dataSource),
        dataSource.getKsqlTopic().getKeyFormat(),
        step
    );
  }

  private static <K> SchemaKStream<K> schemaKStream(
      final PlanBuildContext buildContext,
      final LogicalSchema schema,
      final KeyFormat keyFormat,
      final SourceStep<KStreamHolder<K>> streamSource
  ) {
    return new SchemaKStream<>(
        streamSource,
        schema,
        keyFormat,
        buildContext.getKsqlConfig(),
        buildContext.getFunctionRegistry()
    );
  }

  private static <K> SchemaKTable<K> schemaKTable(
      final PlanBuildContext buildContext,
      final LogicalSchema schema,
      final KeyFormat keyFormat,
      final SourceStep<KTableHolder<K>> tableSource
  ) {
    return new SchemaKTable<>(
        tableSource,
        schema,
        keyFormat,
        buildContext.getKsqlConfig(),
        buildContext.getFunctionRegistry()
    );
  }

  private static LogicalSchema resolveSchema(
      final PlanBuildContext buildContext,
      final ExecutionStep<?> step,
      final DataSource dataSource) {
    return new StepSchemaResolver(
        buildContext.getKsqlConfig(),
        buildContext.getFunctionRegistry()
    ).resolve(step, dataSource.getSchema());
  }
}
