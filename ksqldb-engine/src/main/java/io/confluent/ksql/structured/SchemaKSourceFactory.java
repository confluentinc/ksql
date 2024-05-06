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

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.SourceStep;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.plan.TableSourceV1;
import io.confluent.ksql.execution.plan.WindowedStreamSource;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.execution.streams.StepSchemaResolver;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.planner.plan.PlanBuildContext;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.serde.InternalFormats;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.WindowInfo;
import java.util.Set;
import org.apache.kafka.streams.kstream.Windowed;

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

    final int pseudoColumnVersionToUse = determinePseudoColumnVersionToUse(buildContext);

    final WindowedStreamSource step = ExecutionStepFactory.streamSourceWindowed(
        contextStacker,
        dataSource.getSchema(),
        dataSource.getKafkaTopicName(),
        Formats.from(dataSource.getKsqlTopic()),
        windowInfo,
        dataSource.getTimestampColumn(),
        pseudoColumnVersionToUse
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

    final int pseudoColumnVersionToUse = determinePseudoColumnVersionToUse(buildContext);

    final StreamSource step = ExecutionStepFactory.streamSource(
        contextStacker,
        dataSource.getSchema(),
        dataSource.getKafkaTopicName(),
        Formats.from(dataSource.getKsqlTopic()),
        dataSource.getTimestampColumn(),
        pseudoColumnVersionToUse
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

    final int pseudoColumnVersionToUse = determinePseudoColumnVersionToUse(buildContext);

    final SourceStep<KTableHolder<Windowed<GenericKey>>> step =
        ExecutionStepFactory.tableSourceWindowed(
        contextStacker,
        dataSource.getSchema(),
        dataSource.getKafkaTopicName(),
        Formats.from(dataSource.getKsqlTopic()),
        windowInfo,
        dataSource.getTimestampColumn(),
        pseudoColumnVersionToUse
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

    final KeyFormat keyFormat = dataSource.getKsqlTopic().getKeyFormat();

    if (keyFormat.isWindowed()) {
      throw new IllegalArgumentException("windowed");
    }

    final SourceStep<KTableHolder<GenericKey>> step;

    final int pseudoColumnVersionToUse = determinePseudoColumnVersionToUse(buildContext);

    // If the old query has a v1 table step, continue to use it.
    // See https://github.com/confluentinc/ksql/pull/7990
    boolean useOldExecutionStepVersion = false;
    if (buildContext.getPlanInfo().isPresent()) {
      final Set<ExecutionStep<?>> sourceSteps = buildContext.getPlanInfo().get().getSources();
      useOldExecutionStepVersion = sourceSteps
          .stream()
          .anyMatch(executionStep -> executionStep instanceof TableSourceV1);
    }

    if (!useOldExecutionStepVersion) {
      step = ExecutionStepFactory.tableSource(
          contextStacker,
          dataSource.getSchema(),
          dataSource.getKafkaTopicName(),
          Formats.from(dataSource.getKsqlTopic()),
          dataSource.getTimestampColumn(),
          InternalFormats.of(keyFormat, Formats.from(dataSource.getKsqlTopic()).getValueFormat()),
          pseudoColumnVersionToUse
      );
    } else {
      if (pseudoColumnVersionToUse != SystemColumns.LEGACY_PSEUDOCOLUMN_VERSION_NUMBER) {
        throw new IllegalStateException("TableSourceV2 was released in conjunction with "
            + "pseudocolumn version 1. Something has gone very wrong");
      }
      step = ExecutionStepFactory.tableSourceV1(
          contextStacker,
          dataSource.getSchema(),
          dataSource.getKafkaTopicName(),
          Formats.from(dataSource.getKsqlTopic()),
          dataSource.getTimestampColumn(),
          pseudoColumnVersionToUse
      );
    }

    return schemaKTable(
        buildContext,
        resolveSchema(buildContext, step, dataSource),
        dataSource.getKsqlTopic().getKeyFormat(),
        step
    );
  }

  private static int determinePseudoColumnVersionToUse(final PlanBuildContext buildContext) {

    // Assume statement is CREATE OR REPLACE if this is present, as it indicates that there was
    // an existing query with the same ID. if it wasn't COR, it will fail later
    if (buildContext.getPlanInfo().isPresent()) {
      final Set<ExecutionStep<?>> sourceSteps = buildContext.getPlanInfo().get().getSources();

      return sourceSteps.stream()
          .map(SourceStep.class::cast)
          .mapToInt(SourceStep::getPseudoColumnVersion)
          .findAny().getAsInt();
    }
    return SystemColumns.CURRENT_PSEUDOCOLUMN_VERSION_NUMBER;
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
