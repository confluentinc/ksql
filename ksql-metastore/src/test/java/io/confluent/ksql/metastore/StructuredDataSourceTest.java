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

package io.confluent.ksql.metastore;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.metastore.model.StructuredDataSource;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StructuredDataSourceTest {

  private static final Schema SOME_SCHEMA = SchemaBuilder.struct()
      .field("f0", Schema.OPTIONAL_INT64_SCHEMA)
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

  /**
   * Test class to allow the abstract base class to be instantiated.
   */
  private static final class TestStructuredDataSource extends StructuredDataSource<String> {

    private TestStructuredDataSource(
        final Schema schema,
        final KeyField keyField
    ) {
      super(
          "some SQL",
          "some name",
          schema,
          keyField,
          mock(TimestampExtractionPolicy.class),
          DataSourceType.KSTREAM,
          mock(KsqlTopic.class),
          Serdes::String);
    }
  }
}