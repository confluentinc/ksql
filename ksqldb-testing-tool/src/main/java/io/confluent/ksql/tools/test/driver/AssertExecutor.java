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

package io.confluent.ksql.tools.test.driver;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericKey;
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
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.TabularRow;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.test.TestRecord;

/**
 * {@code AssertExecutor} handles the assertion statements for the Sql-based testing tool.
 */
public final class AssertExecutor {

  @VisibleForTesting
  static final List<SourceProperty> MUST_MATCH = ImmutableList.<SourceProperty>builder()
      .add(new SourceProperty(
          DataSource::getSchema,
          (cs, cfg) -> cs.getElements().toLogicalSchema(),
          "schema"
      )).add(new SourceProperty(
          DataSource::getDataSourceType,
          (cs, cfg) -> cs instanceof CreateTable ? DataSourceType.KTABLE : DataSourceType.KSTREAM,
          "type"
      )).add(new SourceProperty(
          DataSource::getKafkaTopicName,
          (cs, cfg) -> cs.getProperties().getKafkaTopic(),
          "kafka topic",
          CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY
      )).add(new SourceProperty(
          ds -> ds.getKsqlTopic().getKeyFormat().getFormatInfo().getFormat(),
          (cs, cfg) -> cs.getProperties().getKeyFormat(cs.getName()).map(FormatInfo::getFormat)
              .orElse(cfg.getString(KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG)),
          "key format",
          CommonCreateConfigs.KEY_FORMAT_PROPERTY,
          CommonCreateConfigs.FORMAT_PROPERTY
      )).add(new SourceProperty(
          ds -> ds.getKsqlTopic().getKeyFormat().getFormatInfo().getProperties(),
          (cs, cfg) -> cs.getProperties().getKeyFormatProperties(
              cs.getProperties().getKeyFormat(cs.getName()).map(FormatInfo::getFormat)
                  .orElse(cfg.getString(KsqlConfig.KSQL_DEFAULT_KEY_FORMAT_CONFIG)),
              cs.getName().text()
          ),
          "key format properties",
          CommonCreateConfigs.KEY_DELIMITER_PROPERTY,
          CommonCreateConfigs.KEY_SCHEMA_FULL_NAME,
          CommonCreateConfigs.KEY_PROTOBUF_NULLABLE_REPRESENTATION
      )).add(new SourceProperty(
          ds -> ds.getKsqlTopic().getValueFormat().getFormatInfo().getFormat(),
          (cs, cfg) -> cs.getProperties().getValueFormat().map(FormatInfo::getFormat)
              .orElse(cfg.getString(KsqlConfig.KSQL_DEFAULT_VALUE_FORMAT_CONFIG)),
          "value format",
          CommonCreateConfigs.VALUE_FORMAT_PROPERTY,
          CommonCreateConfigs.FORMAT_PROPERTY
      )).add(new SourceProperty(
          ds -> ds.getKsqlTopic().getValueFormat().getFormatInfo().getProperties(),
          (cs, cfg) -> cs.getProperties().getValueFormatProperties(
              cs.getProperties().getValueFormat().map(FormatInfo::getFormat)
                  .orElse(cfg.getString(KsqlConfig.KSQL_DEFAULT_VALUE_FORMAT_CONFIG))
          ),
          "value format properties",
          CommonCreateConfigs.VALUE_AVRO_SCHEMA_FULL_NAME,
          CommonCreateConfigs.VALUE_SCHEMA_FULL_NAME,
          CommonCreateConfigs.VALUE_DELIMITER_PROPERTY,
          CommonCreateConfigs.VALUE_PROTOBUF_NULLABLE_REPRESENTATION
      )).add(new SourceProperty(
          ds -> ds.getKsqlTopic().getValueFormat().getFormatInfo().getProperties()
              .get(CommonCreateConfigs.VALUE_SCHEMA_ID),
          (cs, cfg) -> cs.getProperties().getValueSchemaId().map(String::valueOf).orElse(null),
          "value schema id",
          CommonCreateConfigs.VALUE_SCHEMA_ID))
      .add(new SourceProperty(
          ds -> ds.getKsqlTopic().getKeyFormat().getFormatInfo().getProperties()
              .get(CommonCreateConfigs.KEY_SCHEMA_ID),
          (cs, cfg) -> cs.getProperties().getKeySchemaId().map(String::valueOf).orElse(null),
          "key schema id",
          CommonCreateConfigs.KEY_SCHEMA_ID))
      .add(new SourceProperty(
          ds -> ds.getKsqlTopic().getValueFormat().getFeatures(),
          (cs, cfg) -> cs.getProperties().getValueSerdeFeatures(),
          "value serde features",
          CommonCreateConfigs.WRAP_SINGLE_VALUE
      )).add(new SourceProperty(
          ds -> ds.getTimestampColumn().map(TimestampColumn::getColumn),
          (cs, cfg) -> cs.getProperties().getTimestampColumnName(),
          "timestamp column",
          CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY
      )).add(new SourceProperty(
          ds -> ds.getTimestampColumn().flatMap(TimestampColumn::getFormat),
          (cs, cfg) -> cs.getProperties().getTimestampFormat(),
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
    final DataSource dataSource = engine.getMetaStore().getSource(values.getTarget());
    final KsqlGenericRecord expected = new GenericRecordFactory(
        config, engine.getMetaStore(), System::currentTimeMillis
    ).build(
        values.getColumns(),
        values.getValues(),
        dataSource.getSchema(),
        dataSource.getDataSourceType()
    );

    final Iterator<TestRecord<GenericKey, GenericRow>> records = driverPipeline
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

    final TestRecord<?, GenericRow> actualTestRecord = records.next();
    final GenericKey actualKey = actualTestRecord.key() instanceof Windowed
        ? (GenericKey) ((Windowed) actualTestRecord.key()).key()
        : (GenericKey) actualTestRecord.key();
    final KsqlGenericRecord actual = KsqlGenericRecord.of(
        actualKey,
        actualTestRecord.value(),
        actualTestRecord.timestamp()
    );

    if (!verifyValues(expected, actual, dataSource, values.getColumns(), isTombstone)) {
      throwAssertionError(
          "Expected record does not match actual.",
          dataSource,
          expected,
          ImmutableList.of(actual));
    }
  }

  private static boolean verifyValues(
      final KsqlGenericRecord expected,
      final KsqlGenericRecord actual,
      final DataSource dataSource,
      final List<ColumnName> columns,
      final boolean isTombstone
  ) {
    if (isTombstone) {
      if (expected.value.values().stream().anyMatch(Objects::nonNull)) {
        throw new AssertionError(
            "Unexpected value columns specified in ASSERT NULL VALUES."
        );
      }
      return actual.value == null && actual.key.equals(expected.key);
    }

    if (actual.value == null
        || expected.key.size() != actual.key.size()
        || expected.value.size() != actual.value.size()) {
      return false;
    }

    final LogicalSchema schema = dataSource.getSchema().withPseudoAndKeyColsInValue(false);
    for (final ColumnName col : columns) {
      if (!checkColumn(col, schema, expected, actual)) {
        return false;
      }
    }
    return true;
  }

  private static boolean checkColumn(
      final ColumnName col,
      final LogicalSchema schema,
      final KsqlGenericRecord expected,
      final KsqlGenericRecord actual
  ) {
    if (schema.isKeyColumn(col)) {
      final int index = schema.findColumn(col).get().index();
      if (!expected.key.get(index).equals(actual.key.get(index))) {
        return false;
      }
    }  else if (col.equals(SystemColumns.ROWTIME_NAME)) {
      if (expected.ts != actual.ts) {
        return false;
      }
    } else if (schema.findValueColumn(col).isPresent()) {
      final int index = schema.findColumn(col).get().index();
      if (!expected.value.get(index).equals(actual.value.get(index))) {
        return false;
      }
    }
    return true;
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
    contents.appendAll(row.key.values());
    contents.append(row.ts);
    if (row.value == null) {
      for (final Column ignored : source.getSchema().value()) {
        contents.append("<TOMBSTONE>");
      }
    } else {
      contents.appendAll(row.value.values());
    }

    return TabularRow.createRow(80, contents.values(), false, 0);
  }

  public static void assertStream(
      final KsqlExecutionContext engine,
      final KsqlConfig config,
      final AssertStream assertStatement
  ) {
    assertSourceMatch(engine, config, assertStatement.getStatement());
  }

  public static void assertTable(
      final KsqlExecutionContext engine,
      final KsqlConfig config,
      final AssertTable assertStatement
  ) {
    assertSourceMatch(engine, config, assertStatement.getStatement());
  }


  private static void assertSourceMatch(
      final KsqlExecutionContext engine,
      final KsqlConfig config,
      final CreateSource statement
  ) {
    final SourceName source = statement.getName();
    final DataSource dataSource = engine.getMetaStore().getSource(source);

    MUST_MATCH.forEach(prop -> prop.compare(dataSource, statement, config));
  }

  @VisibleForTesting
  static final class SourceProperty {

    final Function<DataSource, Object> extractSource;
    final BiFunction<CreateSource, KsqlConfig, Object> extractStatement;
    final String propertyName;
    final String[] withClauseName;

    private SourceProperty(
        final Function<DataSource, Object> extractSource,
        final BiFunction<CreateSource, KsqlConfig, Object> extractStatement,
        final String propertyName,
        final String... withClauseName
    ) {
      this.extractSource = extractSource;
      this.extractStatement = extractStatement;
      this.propertyName = propertyName;
      this.withClauseName = withClauseName;
    }

    private void compare(
        final DataSource dataSource,
        final CreateSource statement,
        final KsqlConfig config
    ) {
      final Object expected = extractStatement.apply(statement, config);
      final Object actual = extractSource.apply(dataSource);

      if (!Objects.equals(actual, expected)) {
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
