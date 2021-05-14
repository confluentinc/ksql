/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.parser.json;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.junit.BeforeClass;
import org.junit.Test;

public class LogicalSchemaDeserializerTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @BeforeClass
  public static void classSetUp() {
    MAPPER.registerModule(new TestModule());
  }

  @Test
  public void shouldDeserializeSchemaWithImplicitColumns() throws Exception {
    // Given:
    final String json = "\"`ROWKEY` STRING KEY, `v0` INTEGER\"";

    // When:
    final LogicalSchema schema = MAPPER.readValue(json, LogicalSchema.class);

    // Then:
    assertThat(schema, is(LogicalSchema.builder()
        .keyColumn(ColumnName.of("ROWKEY"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("v0"), SqlTypes.INTEGER)
        .build()));
  }

  @Test
  public void shouldDeserializeSchemaWithOutImplicitColumns() throws Exception {
    // Given:
    final String json = "\"`key` STRING KEY, `v0` INTEGER\"";

    // When:
    final LogicalSchema schema = MAPPER.readValue(json, LogicalSchema.class);

    // Then:
    assertThat(schema, is(LogicalSchema.builder()
        .keyColumn(ColumnName.of("key"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("v0"), SqlTypes.INTEGER)
        .build()));
  }

  @Test
  public void shouldDeserializeSchemaWithKeyAfterValue() throws Exception {
    // Given:
    final String json = "\"`v0` INTEGER, `key0` STRING KEY\"";

    // When:
    final LogicalSchema schema = MAPPER.readValue(json, LogicalSchema.class);

    // Then:
    assertThat(schema, is(LogicalSchema.builder()
        .valueColumn(ColumnName.of("v0"), SqlTypes.INTEGER)
        .keyColumn(ColumnName.of("key0"), SqlTypes.STRING)
        .build()));
  }

  private static class TestModule extends SimpleModule {

    private TestModule() {
      addDeserializer(
          LogicalSchema.class,
          new LogicalSchemaDeserializer()
      );
    }
  }
}
