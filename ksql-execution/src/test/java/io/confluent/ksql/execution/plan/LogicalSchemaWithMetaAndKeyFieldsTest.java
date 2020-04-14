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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.junit.Test;

public class LogicalSchemaWithMetaAndKeyFieldsTest {
  private static final SourceName ALIAS = SourceName.of("alias");
  private static final LogicalSchema ORIGINAL = LogicalSchema.builder()
      .valueColumn(ColumnName.of("field1"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("field2"), SqlTypes.BIGINT)
      .build();

  private final LogicalSchemaWithMetaAndKeyFields schema
      = LogicalSchemaWithMetaAndKeyFields.fromOriginal(ALIAS, ORIGINAL);

  @Test
  public void shouldTransformSchemaCorrectly() {
    assertThat(
        schema.getSchema(),
        equalTo(ORIGINAL.withAlias(ALIAS).withMetaAndKeyColsInValue()));
  }

  @Test
  public void shouldReturnoriginalSchema() {
    assertThat(schema.getOriginalSchema(), equalTo(ORIGINAL));
  }
}