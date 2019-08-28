/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.execution.plan;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.schema.ksql.LogicalSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

public class LogicalSchemaWithMetaAndKeyFieldsTest {
  private static final String ALIAS = "alias";
  private static final LogicalSchema ORIGINAL = LogicalSchema.of(SchemaBuilder.struct()
      .field("field1", Schema.OPTIONAL_STRING_SCHEMA)
      .field("field2", Schema.OPTIONAL_INT64_SCHEMA)
      .build());

  private final LogicalSchemaWithMetaAndKeyFields schema
      = LogicalSchemaWithMetaAndKeyFields.fromOriginal(ALIAS, ORIGINAL);

  @Test
  public void shouldTransformSchemaCorrectly() {
    assertThat(
        schema.getSchema(),
        equalTo(ORIGINAL.withAlias(ALIAS).withMetaAndKeyFieldsInValue()));
  }

  @Test
  public void shouldReturnoriginalSchema() {
    assertThat(schema.getOriginalSchema(), equalTo(ORIGINAL));
  }
}