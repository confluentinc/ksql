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
import static org.mockito.Mockito.verify;

import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StructuredDataSourceTest {

  private static final LogicalSchema SOME_SCHEMA = LogicalSchema.builder()
      .valueField("f0", SqlTypes.BIGINT)
      .build();

  @Mock
  public KeyField keyField;

  @Test
  public void shouldValidateKeyFieldIsInSchema() {
    // When:
    new TestStructuredDataSource(
        SOME_SCHEMA,
        keyField
    );

    // Then (no exception):
    verify(keyField).validateKeyExistsIn(SOME_SCHEMA);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfSchemaContainsRowTime() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueField(SchemaUtil.ROWTIME_NAME, SqlTypes.BIGINT)
        .valueField("f0", SqlTypes.BIGINT)
        .build();

    // When:
    new TestStructuredDataSource(
        schema,
        keyField
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfSchemaContainsRowKey() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueField(SchemaUtil.ROWKEY_NAME, SqlTypes.STRING)
        .valueField("f0", SqlTypes.BIGINT)
        .build();

    // When:
    new TestStructuredDataSource(
        schema,
        keyField
    );
  }

  /**
   * Test class to allow the abstract base class to be instantiated.
   */
  private static final class TestStructuredDataSource extends StructuredDataSource<String> {

    private TestStructuredDataSource(
        final LogicalSchema schema,
        final KeyField keyField
    ) {
      super(
          "some SQL",
          "some name",
          schema,
          SerdeOption.none(), keyField,
          mock(TimestampExtractionPolicy.class),
          DataSourceType.KSTREAM,
          mock(KsqlTopic.class)
      );
    }
  }
}