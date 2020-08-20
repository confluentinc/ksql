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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.engine.generic.GenericRecordFactory;
import io.confluent.ksql.engine.generic.KsqlGenericRecord;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.parser.AssertTable;
import io.confluent.ksql.parser.tree.AssertStream;
import io.confluent.ksql.parser.tree.AssertValues;
import io.confluent.ksql.parser.tree.InsertValues;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Iterator;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.test.TestRecord;

/**
 * {@code AssertExecutor} handles the assertion statements for the Sql-based
 * testing tool.
 */
public final class AssertExecutor {

  private AssertExecutor() {
  }

  public static void assertValues(
      final KsqlExecutionContext engine,
      final KsqlConfig config,
      final AssertValues assertValues,
      final TestDriverPipeline driverPipeline
  ) {
    final InsertValues values = assertValues.getStatement();
    final boolean compareTimestamp = values
        .getColumns()
        .stream()
        .anyMatch(SystemColumns.ROWTIME_NAME::equals);

    final DataSource dataSource = engine.getMetaStore().getSource(values.getTarget());
    final KsqlGenericRecord expected = new GenericRecordFactory(
        config, engine.getMetaStore(), System::currentTimeMillis
    ).build(
        values.getColumns(),
        values.getValues(),
        dataSource.getSchema(),
        dataSource.getDataSourceType()
    );

    final Iterator<TestRecord<Struct, GenericRow>> records = driverPipeline
        .getRecordsForTopic(dataSource.getKafkaTopicName());
    if (!records.hasNext()) {
      throw new KsqlException(
          String.format(
              "Expected another record (%s) for %s but already read all records: %s",
              expected,
              dataSource.getName(),
              driverPipeline.getAllRecordsForTopic(dataSource.getKafkaTopicName())
          )
      );
    }

    final TestRecord<Struct, GenericRow> actualTestRecord = records.next();
    final KsqlGenericRecord actual = KsqlGenericRecord.of(
        actualTestRecord.key(),
        actualTestRecord.value(),
        compareTimestamp ? actualTestRecord.timestamp() : expected.ts
    );

    if (!actual.equals(expected)) {
      throw new KsqlException(
          String.format(
              "Expected record does not match actual. Expected: %s vs. Actual: %s",
              expected,
              actual
          )
      );
    }
  }

  public static void assertStream(final AssertStream assertStatement) {
    throw new UnsupportedOperationException();
  }

  public static void assertTable(final AssertTable assertStatement) {
    throw new UnsupportedOperationException();
  }

}
