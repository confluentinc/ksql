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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.plan.AbstractStreamSource;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.LogicalSchemaWithMetaAndKeyFields;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.plan.TableSource;
import io.confluent.ksql.execution.plan.WindowedStreamSource;
import io.confluent.ksql.execution.plan.WindowedTableSource;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.serde.KeyFormat;
import java.util.Optional;
import org.apache.kafka.streams.Topology.AutoOffsetReset;

/**
 * Factor class used to create stream and table sources
 */
public final class SchemaKSourceFactory {

  private SchemaKSourceFactory() {
  }

  public static SchemaKStream<?> buildSource(
      final KsqlQueryBuilder builder,
      final DataSource<?> dataSource,
      final LogicalSchemaWithMetaAndKeyFields schemaWithMetaAndKeyFields,
      final QueryContext.Stacker contextStacker,
      final Optional<AutoOffsetReset> offsetReset,
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
            schemaWithMetaAndKeyFields,
            contextStacker,
            offsetReset,
            keyField,
            alias
        ) : buildStream(
            builder,
            dataSource,
            schemaWithMetaAndKeyFields,
            contextStacker,
            offsetReset,
            keyField,
            alias
        );

      case KTABLE:
        return windowed
            ? buildWindowedTable(
            builder,
            dataSource,
            schemaWithMetaAndKeyFields,
            contextStacker,
            offsetReset,
            keyField,
            alias
        ) : buildTable(
            builder,
            dataSource,
            schemaWithMetaAndKeyFields,
            contextStacker,
            offsetReset,
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
      final LogicalSchemaWithMetaAndKeyFields schemaWithMetaAndKeyFields,
      final Stacker contextStacker,
      final Optional<AutoOffsetReset> offsetReset,
      final KeyField keyField,
      final SourceName alias
  ) {
    final WindowedStreamSource step = ExecutionStepFactory.streamSourceWindowed(
        contextStacker,
        schemaWithMetaAndKeyFields,
        dataSource.getKafkaTopicName(),
        buildFormats(dataSource),
        dataSource.getTimestampColumn(),
        offsetReset,
        alias
    );

    return schemaKStream(
        builder,
        dataSource.getKsqlTopic().getKeyFormat(),
        step,
        keyField
    );
  }

  private static SchemaKStream<?> buildStream(
      final KsqlQueryBuilder builder,
      final DataSource<?> dataSource,
      final LogicalSchemaWithMetaAndKeyFields schemaWithMetaAndKeyFields,
      final Stacker contextStacker,
      final Optional<AutoOffsetReset> offsetReset,
      final KeyField keyField,
      final SourceName alias
  ) {
    final StreamSource step = ExecutionStepFactory.streamSource(
        contextStacker,
        schemaWithMetaAndKeyFields,
        dataSource.getKafkaTopicName(),
        buildFormats(dataSource),
        dataSource.getTimestampColumn(),
        offsetReset,
        alias
    );

    return schemaKStream(
        builder,
        dataSource.getKsqlTopic().getKeyFormat(),
        step,
        keyField
    );
  }

  private static SchemaKTable<?> buildWindowedTable(
      final KsqlQueryBuilder builder,
      final DataSource<?> dataSource,
      final LogicalSchemaWithMetaAndKeyFields schemaWithMetaAndKeyFields,
      final Stacker contextStacker,
      final Optional<AutoOffsetReset> offsetReset,
      final KeyField keyField,
      final SourceName alias
  ) {
    final WindowedTableSource step = ExecutionStepFactory.tableSourceWindowed(
        contextStacker,
        schemaWithMetaAndKeyFields,
        dataSource.getKafkaTopicName(),
        buildFormats(dataSource),
        dataSource.getTimestampColumn(),
        offsetReset,
        alias
    );

    return schemaKTable(
        builder,
        dataSource.getKsqlTopic().getKeyFormat(),
        step,
        keyField
    );
  }

  private static SchemaKTable<?> buildTable(
      final KsqlQueryBuilder builder,
      final DataSource<?> dataSource,
      final LogicalSchemaWithMetaAndKeyFields schemaWithMetaAndKeyFields,
      final Stacker contextStacker,
      final Optional<AutoOffsetReset> offsetReset,
      final KeyField keyField,
      final SourceName alias
  ) {
    final TableSource step = ExecutionStepFactory.tableSource(
        contextStacker,
        schemaWithMetaAndKeyFields,
        dataSource.getKafkaTopicName(),
        buildFormats(dataSource),
        dataSource.getTimestampColumn(),
        offsetReset,
        alias
    );

    return schemaKTable(
        builder,
        dataSource.getKsqlTopic().getKeyFormat(),
        step,
        keyField
    );
  }

  private static <K> SchemaKStream<K> schemaKStream(
      final KsqlQueryBuilder builder,
      final KeyFormat keyFormat,
      final AbstractStreamSource<KStreamHolder<K>> streamSource,
      final KeyField keyField
  ) {
    return new SchemaKStream<>(
        streamSource,
        keyFormat,
        keyField,
        ImmutableList.of(),
        SchemaKStream.Type.SOURCE,
        builder.getKsqlConfig(),
        builder.getFunctionRegistry()
    );
  }

  private static <K> SchemaKTable<K> schemaKTable(
      final KsqlQueryBuilder builder,
      final KeyFormat keyFormat,
      final AbstractStreamSource<KTableHolder<K>> tableSource,
      final KeyField keyField
  ) {
    return new SchemaKTable<>(
        tableSource,
        keyFormat,
        keyField,
        ImmutableList.of(),
        SchemaKStream.Type.SOURCE,
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
}
