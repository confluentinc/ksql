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

import com.google.common.collect.Iterables;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import org.junit.Test;

public class JoinParamsFactoryTest {

  private static final LogicalSchema LEFT_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("L_K"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("L_BLUE"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("L_GREEN"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("L_K"), SqlTypes.STRING) // Copy of key in value
      .build();

  private static final LogicalSchema RIGHT_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("R_K"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("R_RED"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("R_ORANGE"), SqlTypes.DOUBLE)
      .valueColumn(ColumnName.of("R_K"), SqlTypes.STRING) // Copy of key in value
      .build();

  @Test
  public void shouldBuildCorrectLeftKeyedSchema() {
    // Given:
    final ColumnName keyName = Iterables.getOnlyElement(LEFT_SCHEMA.key()).name();

    // When:
    final JoinParams joinParams = JoinParamsFactory.create(keyName, LEFT_SCHEMA, RIGHT_SCHEMA);

    // Then:
    assertThat(joinParams.getSchema(), is(LogicalSchema.builder()
        .keyColumn(ColumnName.of("L_K"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("L_BLUE"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("L_GREEN"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("L_K"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("R_RED"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("R_ORANGE"), SqlTypes.DOUBLE)
        .valueColumn(ColumnName.of("R_K"), SqlTypes.STRING)
        .build())
    );
  }

  @Test
  public void shouldBuildCorrectRightKeyedSchema() {
    // Given:
    final ColumnName keyName = Iterables.getOnlyElement(RIGHT_SCHEMA.key()).name();

    // When:
    final JoinParams joinParams = JoinParamsFactory.create(keyName, LEFT_SCHEMA, RIGHT_SCHEMA);

    // Then:
    assertThat(joinParams.getSchema(), is(LogicalSchema.builder()
        .keyColumn(ColumnName.of("R_K"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("L_BLUE"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("L_GREEN"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("L_K"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("R_RED"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("R_ORANGE"), SqlTypes.DOUBLE)
        .valueColumn(ColumnName.of("R_K"), SqlTypes.STRING)
        .build())
    );
  }

  @Test
  public void shouldBuildCorrectSyntheticKeyedSchema() {
    // Given:
    final ColumnName keyName = ColumnName.of("OTHER");

    // When:
    final JoinParams joinParams = JoinParamsFactory.create(keyName, LEFT_SCHEMA, RIGHT_SCHEMA);

    // Then:
    assertThat(joinParams.getSchema(), is(LogicalSchema.builder()
        .keyColumn(keyName, SqlTypes.STRING)
        .valueColumn(ColumnName.of("L_BLUE"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("L_GREEN"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("L_K"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("R_RED"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("R_ORANGE"), SqlTypes.DOUBLE)
        .valueColumn(ColumnName.of("R_K"), SqlTypes.STRING)
        .valueColumn(keyName, SqlTypes.STRING)
        .build())
    );
  }

  @Test
  public void shouldThrowOnKeyTypeMismatch() {
    // Given:
    final LogicalSchema intKeySchema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("BOB"), SqlTypes.INTEGER)
        .valueColumn(ColumnName.of("BLUE"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("GREEN"), SqlTypes.INTEGER)
        .build()
        .withPseudoAndKeyColsInValue(false);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> JoinParamsFactory.create(ColumnName.of("BOB"), intKeySchema, RIGHT_SCHEMA)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Invalid join. Key types differ: INTEGER vs STRING"));
  }
}