/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.test.driver;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.engine.generic.GenericRecordFactory;
import io.confluent.ksql.engine.generic.KsqlGenericRecord;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.AssertTable;
import io.confluent.ksql.parser.tree.AssertStream;
import io.confluent.ksql.parser.tree.AssertTombstone;
import io.confluent.ksql.parser.tree.AssertValues;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.TabularRow;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.test.TestRecord;

/**
 * {@code AssertExecutor} handles the assertion statements for the Sql-based
 * testing tool.
 */
public final class AssertExecutor {

  @VisibleForTesting
  static final List<SourceProperty> MUST_MATCH = ImmutableList.<SourceProperty>builder()
      .add(new SourceProperty(
          DataSource::getSchema,
          cs -> cs.getElements().toLogicalSchema(),
          "schema"
      )).add(new SourceProperty(
          DataSource::getDataSourceType,
          cs -> cs instanceof CreateTable ? DataSourceType.KTABLE : DataSourceType.KSTREAM,
          "type"
      )).add(new SourceProperty(
          DataSource::getKafkaTopicName,
          cs -> cs.getProperties().getKafkaTopic(),
          "kafka topic",
          CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY
      )).add(new SourceProperty(
          ds -> ds.getKsqlTopic().getValueFormat().getFormatInfo().getFormat(),
          cs -> cs.getProperties().getValueFormat().name(),
          "value format",
          CommonCreateConfigs.VALUE_FORMAT_PROPERTY
      )).add(new SourceProperty(
          ds -> ds.getKsqlTopic().getValueFormat().getFormatInfo().getProperties(),
          cs -> cs.getProperties().getFormatInfo().getProperties(),
          "delimiter",
          CommonCreateConfigs.VALUE_AVRO_SCHEMA_FULL_NAME,
          CommonCreateConfigs.VALUE_DELIMITER_PROPERTY
      )).add(new SourceProperty(
          DataSource::getSerdeOptions,
          cs -> cs.getProperties().getSerdeOptions(),
          "serde options",
          CommonCreateConfigs.WRAP_SINGLE_VALUE
      )).add(new SourceProperty(
          ds -> ds.getTimestampColumn().map(TimestampColumn::getColumn),
          cs -> cs.getProperties().getTimestampColumnName(),
          "timestamp column",
          CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY
      )).add(new SourceProperty(
          ds -> ds.getTimestampColumn().flatMap(TimestampColumn::getFormat),
          cs -> cs.getProperties().getTimestampFormat(),
          "timestamp format",
          CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY
      )).build();

  private AssertExecutor() {
  }

  public static void assertValues(
      final KsqlExecutionContext engine,
      final KsqlConfig config,
      final AssertValues assertValues,
      final TestDriverPipeline driverPipeline
  ) {
    final InsertValues values = assertValues.getStatement();
    assertContent(engine, config, values, driverPipeline, false);
  }

  public static void assertTombstone(
      final KsqlExecutionContext engine,
      final KsqlConfig config,
      final AssertTombstone assertTombstone,
      final TestDriverPipeline driverPipeline
  ) {
    final InsertValues values = assertTombstone.getStatement();
    assertContent(engine, config, values, driverPipeline, true);
  }

  private static void assertContent(
      final KsqlExecutionContext engine,
      final KsqlConfig config,
      final InsertValues values,
      final TestDriverPipeline driverPipeline,
      final boolean isTombstone
  ) {
    final boolean compareTimestamp = values
        .getColumns()
        .stream()
        .anyMatch(SystemColumns.ROWTIME_NAME::equals);

    final DataSource dataSource = engine.getMetaStore().getSource(values.getTarget());
    KsqlGenericRecord expected = new GenericRecordFactory(
        config, engine.getMetaStore(), System::currentTimeMillis
    ).build(
        values.getColumns(),
        values.getValues(),
        dataSource.getSchema(),
        dataSource.getDataSourceType()
    );

    if (isTombstone) {
      if (expected.value.values().stream().anyMatch(Objects::nonNull)) {
        throw new AssertionError("Unexpected value columns specified in ASSERT NULL VALUES.");
      }
      expected = KsqlGenericRecord.of(expected.key, null, expected.ts);
    }

    final Iterator<TestRecord<Struct, GenericRow>> records = driverPipeline
        .getRecordsForTopic(dataSource.getKafkaTopicName());
    if (!records.hasNext()) {
      throwAssertionError(
          "Expected another record, but all records have already been read:",
          dataSource,
          expected,
          driverPipeline.getAllRecordsForTopic(dataSource.getKafkaTopicName())
              .stream()
              .map(rec -> KsqlGenericRecord.of(rec.key(), rec.value(), rec.timestamp()))
              .collect(Collectors.toList())
      );
    }

    final TestRecord<Struct, GenericRow> actualTestRecord = records.next();
    final KsqlGenericRecord actual = KsqlGenericRecord.of(
        actualTestRecord.key(),
        actualTestRecord.value(),
        compareTimestamp ? actualTestRecord.timestamp() : expected.ts
    );

    if (!actual.equals(expected)) {
      throwAssertionError(
          "Expected record does not match actual.",
          dataSource,
          expected,
          ImmutableList.of(actual));
    }
  }

  private static void throwAssertionError(
      final String message,
      final DataSource dataSource,
      final KsqlGenericRecord expected,
      final List<KsqlGenericRecord> actual
  ) {
    final List<Column> columns = ImmutableList.<Column>builder()
        .add(Column.of(ColumnName.of("."), SqlTypes.STRING, Namespace.KEY, 0))
        .add(Column.of(SystemColumns.ROWTIME_NAME, SqlTypes.BIGINT, Namespace.KEY, 0))
        .addAll(dataSource.getSchema().columns())
        .build();

    final TabularRow headerRow = TabularRow.createHeader(80, columns, false, 0);

    final StringBuilder actualRows = new StringBuilder();
    actual.forEach(a -> actualRows.append(fromGenericRow(false, dataSource, a)).append('\n'));
    throw new AssertionError(
        String.format(
            "%s%n%s%n%s%n%s",
            message,
            headerRow,
            fromGenericRow(true, dataSource, expected),
            actualRows.toString()
        )
    );
  }

  private static TabularRow fromGenericRow(
      final boolean expected,
      final DataSource source,
      final KsqlGenericRecord row
  ) {
    final GenericRow contents = new GenericRow();
    contents.append(expected ? "EXPECTED" : "ACTUAL");
    for (final Column key : source.getSchema().key()) {
      contents.append(row.key.get(key.name().text()));
    }
    contents.append(row.ts);
    contents.appendAll(row.value.values());

    return TabularRow.createRow(80, contents, false, 0);
  }

  public static void assertStream(
      final KsqlExecutionContext engine,
      final AssertStream assertStatement
  ) {
    assertSourceMatch(engine, assertStatement.getStatement());
  }

  public static void assertTable(
      final KsqlExecutionContext engine,
      final AssertTable assertStatement
  ) {
    assertSourceMatch(engine, assertStatement.getStatement());
  }


  private static void assertSourceMatch(
      final KsqlExecutionContext engine,
      final CreateSource statement
  ) {
    final SourceName source = statement.getName();
    final DataSource dataSource = engine.getMetaStore().getSource(source);

    MUST_MATCH.forEach(prop -> prop.compare(dataSource, statement));
  }

  @VisibleForTesting
  static final class SourceProperty {
    final Function<DataSource, Object> extractSource;
    final Function<CreateSource, Object> extractStatement;
    final String propertyName;
    final String[] withClauseName;

    private SourceProperty(
        final Function<DataSource, Object> extractSource,
        final Function<CreateSource, Object> extractStatement,
        final String propertyName,
        final String... withClauseName
    ) {
      this.extractSource = extractSource;
      this.extractStatement = extractStatement;
      this.propertyName = propertyName;
      this.withClauseName = withClauseName;
    }

    private void compare(final DataSource dataSource, final CreateSource statement) {
      final Object expected = extractStatement.apply(statement);
      final Object actual = extractSource.apply(dataSource);
      if (!actual.equals(expected)) {
        throw new KsqlException(
            String.format(
                "Expected %s does not match actual for source %s.%n\tExpected: %s%n\tActual: %s",
                propertyName,
                dataSource.getName().toString(FormatOptions.noEscape()),
                expected,
                actual
            )
        );
      }
    }
  }

}
