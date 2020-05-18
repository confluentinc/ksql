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

package io.confluent.ksql.metastore.model;

import static org.mockito.Mockito.mock;

import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeOption;
import java.util.Optional;
import org.junit.Test;

public class StructuredDataSourceTest {

  private static final LogicalSchema SOME_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k0"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("f0"), SqlTypes.BIGINT)
      .build();

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfSchemaContainsRowTime() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("k0"), SqlTypes.INTEGER)
        .valueColumn(SystemColumns.ROWTIME_NAME, SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("f0"), SqlTypes.BIGINT)
        .build();

    // When:
    new TestStructuredDataSource(schema);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnDuplicateColumnNames() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("dup"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("dup"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("f0"), SqlTypes.BIGINT)
        .build();

    // When:
    new TestStructuredDataSource(schema);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfSchemaContainsWindowStart() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("k0"), SqlTypes.INTEGER)
        .valueColumn(SystemColumns.WINDOWSTART_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("f0"), SqlTypes.BIGINT)
        .build();

    // When:
    new TestStructuredDataSource(schema);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfSchemaContainsWindowEnd() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("k0"), SqlTypes.INTEGER)
        .valueColumn(SystemColumns.WINDOWEND_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("f0"), SqlTypes.BIGINT)
        .build();

    // When:
    new TestStructuredDataSource(schema);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfSchemaContainsValueColumnsWithSameNameAsKeyColumns() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("k0"), SqlTypes.INTEGER)
        .valueColumn(SystemColumns.WINDOWEND_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("k0"), SqlTypes.BIGINT)
        .build();

    // When:
    new TestStructuredDataSource(schema);
  }

  /**
   * Test class to allow the abstract base class to be instantiated.
   */
  private static final class TestStructuredDataSource extends StructuredDataSource<String> {

    private TestStructuredDataSource(
        final LogicalSchema schema
    ) {
      super(
          "some SQL",
          SourceName.of("some name"),
          schema,
          SerdeOption.none(),
          Optional.empty(),
          DataSourceType.KSTREAM,
          false,
          mock(KsqlTopic.class)
      );
    }
  }
}