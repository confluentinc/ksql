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

package io.confluent.ksql.execution.streams;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.junit.Test;

public class ForeignKeyJoinParamsFactoryTest {

  private static final LogicalSchema LEFT_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("L_K"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("L_BLUE"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("L_FOREIGN_KEY"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("L_K"), SqlTypes.STRING)
      .build();

  private static final LogicalSchema RIGHT_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("R_K"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("R_RED"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("R_ORANGE"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("R_K"), SqlTypes.INTEGER)
      .build();

  @Test
  public void shouldBuildCorrectKeyedSchema() {
    // Given:
    final ColumnName leftJoinColumnName = ColumnName.of("L_FOREIGN_KEY");

    // When:
    final ForeignKeyJoinParams<String> joinParams =
        ForeignKeyJoinParamsFactory.create(leftJoinColumnName, LEFT_SCHEMA, RIGHT_SCHEMA);

    // Then:
    assertThat(joinParams.getSchema(), is(LogicalSchema.builder()
        .keyColumn(ColumnName.of("L_K"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("L_BLUE"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("L_FOREIGN_KEY"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("L_K"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("R_RED"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("R_ORANGE"), SqlTypes.DOUBLE)
        .valueColumn(ColumnName.of("R_K"), SqlTypes.INTEGER)
        .build())
    );
  }

  @Test
  public void shouldThrowIfJoinColumnNotFound() {
    // Given:
    final ColumnName leftJoinColumnName = ColumnName.of("L_UNKNOWN");

    final Exception e = assertThrows(
        IllegalStateException.class,
        () -> ForeignKeyJoinParamsFactory.create(leftJoinColumnName, LEFT_SCHEMA, RIGHT_SCHEMA)
    );

    // Then:
    assertThat(
        e.getMessage(),
        containsString("Could not find join column in left input table.")
    );
  }
}