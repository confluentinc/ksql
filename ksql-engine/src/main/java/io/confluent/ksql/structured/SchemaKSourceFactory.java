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

import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.plan.AbstractStreamSource;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.plan.TableSource;
import io.confluent.ksql.execution.plan.WindowedStreamSource;
import io.confluent.ksql.execution.plan.WindowedTableSource;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.execution.streams.StepSchemaResolver;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.SourceName;
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
      final KsqlQueryBuilder builder,
      final DataSource<?> dataSource,
      final QueryContext.Stacker contextStacker,
      final KeyField keyField,
      final SourceName alias
  ) {
    final boolean windowed = dataSource.getKsqlTopic().getKeyFormat().isWindowed();
    switch (dataSource.getDataSourceType()) {
      case KSTREAM:
        return windowed
            ? buildWindowedStream(
            builder,
            dataSource,
            contextStacker,
            keyField,
            alias
        ) : buildStream(
            builder,
            dataSource,
            contextStacker,
            keyField,
            alias
        );

      case KTABLE:
        return windowed
            ? buildWindowedTable(
            builder,
            dataSource,
            contextStacker,
            keyField,
            alias
        ) : buildTable(
            builder,
            dataSource,
            contextStacker,
            keyField,
            alias
        );

      default:
        throw new UnsupportedOperationException("Source type:" + dataSource.getDataSourceType());
    }
  }

  private static SchemaKStream<?> buildWindowedStream(
      final KsqlQueryBuilder builder,
      final DataSource<?> dataSource,
      final Stacker contextStacker,
      final KeyField keyField,
      final SourceName alias
  ) {
    final WindowInfo windowInfo = dataSource.getKsqlTopic().getKeyFormat().getWindowInfo()
        .orElseThrow(IllegalArgumentException::new);

    final WindowedStreamSource step = ExecutionStepFactory.streamSourceWindowed(
        contextStacker,
        dataSource.getSchema(),
        dataSource.getKafkaTopicName(),
        buildFormats(dataSource),
        windowInfo,
        dataSource.getTimestampColumn(),
        alias
    );

    return schemaKStream(
        builder,
        resolveSchema(builder, step, dataSource),
        dataSource.getKsqlTopic().getKeyFormat(),
        step,
        keyField
    );
  }

  private static SchemaKStream<?> buildStream(
      final KsqlQueryBuilder builder,
      final DataSource<?> dataSource,
      final Stacker contextStacker,
      final KeyField keyField,
      final SourceName alias
  ) {
    if (dataSource.getKsqlTopic().getKeyFormat().getWindowInfo().isPresent()) {
      throw new IllegalArgumentException("windowed");
    }

    final StreamSource step = ExecutionStepFactory.streamSource(
        contextStacker,
        dataSource.getSchema(),
        dataSource.getKafkaTopicName(),
        buildFormats(dataSource),
        dataSource.getTimestampColumn(),
        alias
    );

    return schemaKStream(
        builder,
        resolveSchema(builder, step, dataSource),
        dataSource.getKsqlTopic().getKeyFormat(),
        step,
        keyField
    );
  }

  private static SchemaKTable<?> buildWindowedTable(
      final KsqlQueryBuilder builder,
      final DataSource<?> dataSource,
      final Stacker contextStacker,
      final KeyField keyField,
      final SourceName alias
  ) {
    final WindowInfo windowInfo = dataSource.getKsqlTopic().getKeyFormat().getWindowInfo()
        .orElseThrow(IllegalArgumentException::new);

    final WindowedTableSource step = ExecutionStepFactory.tableSourceWindowed(
        contextStacker,
        dataSource.getSchema(),
        dataSource.getKafkaTopicName(),
        buildFormats(dataSource),
        windowInfo,
        dataSource.getTimestampColumn(),
        alias
    );

    return schemaKTable(
        builder,
        resolveSchema(builder, step, dataSource),
        dataSource.getKsqlTopic().getKeyFormat(),
        step,
        keyField
    );
  }

  private static SchemaKTable<?> buildTable(
      final KsqlQueryBuilder builder,
      final DataSource<?> dataSource,
      final Stacker contextStacker,
      final KeyField keyField,
      final SourceName alias
  ) {
    if (dataSource.getKsqlTopic().getKeyFormat().getWindowInfo().isPresent()) {
      throw new IllegalArgumentException("windowed");
    }

    final TableSource step = ExecutionStepFactory.tableSource(
        contextStacker,
        dataSource.getSchema(),
        dataSource.getKafkaTopicName(),
        buildFormats(dataSource),
        dataSource.getTimestampColumn(),
        alias
    );

    return schemaKTable(
        builder,
        resolveSchema(builder, step, dataSource),
        dataSource.getKsqlTopic().getKeyFormat(),
        step,
        keyField
    );
  }

  private static <K> SchemaKStream<K> schemaKStream(
      final KsqlQueryBuilder builder,
      final LogicalSchema schema,
      final KeyFormat keyFormat,
      final AbstractStreamSource<KStreamHolder<K>> streamSource,
      final KeyField keyField
  ) {
    return new SchemaKStream<>(
        streamSource,
        schema,
        keyFormat,
        keyField,
        builder.getKsqlConfig(),
        builder.getFunctionRegistry()
    );
  }

  private static <K> SchemaKTable<K> schemaKTable(
      final KsqlQueryBuilder builder,
      final LogicalSchema schema,
      final KeyFormat keyFormat,
      final AbstractStreamSource<KTableHolder<K>> tableSource,
      final KeyField keyField
  ) {
    return new SchemaKTable<>(
        tableSource,
        schema,
        keyFormat,
        keyField,
        builder.getKsqlConfig(),
        builder.getFunctionRegistry()
    );
  }

  private static Formats buildFormats(final DataSource<?> dataSource) {
    return Formats.of(
        dataSource.getKsqlTopic().getKeyFormat(),
        dataSource.getKsqlTopic().getValueFormat(),
        dataSource.getSerdeOptions()
    );
  }

  private static LogicalSchema resolveSchema(
      final KsqlQueryBuilder queryBuilder,
      final ExecutionStep<?> step,
      final DataSource dataSource) {
    return new StepSchemaResolver(queryBuilder.getKsqlConfig(), queryBuilder.getFunctionRegistry())
        .resolve(step, dataSource.getSchema());
  }
}
