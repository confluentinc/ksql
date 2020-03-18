/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.name;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import org.junit.Test;

public class ColumnNamesTest {

  @Test
  public void shouldGenerateUniqueAliasesStartingAtZero() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .build();

    // When:
    final ColumnName result = ColumnNames.nextGeneratedColumnAlias(schema);

    // Then:
    assertThat(result, is(ColumnName.of("KSQL_COL_0")));
  }

  @Test
  public void shouldGenerateUniqueAliasesTakingAnyKeyColumnsIntoAccount() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(ColumnName.of("Fred"), SqlTypes.STRING)
        .keyColumn(ColumnNames.generatedColumnAlias(1), SqlTypes.STRING)
        .keyColumn(ColumnName.of("George"), SqlTypes.STRING)
        .build();

    // When:
    final ColumnName result = ColumnNames.nextGeneratedColumnAlias(schema);

    // Then:
    assertThat(result, is(ColumnName.of("KSQL_COL_2")));
  }

  @Test
  public void shouldGenerateUniqueAliasesTakingAnyValueColumnsIntoAccount() {
    // Given:
    final LogicalSchema schema = LogicalSchema.builder()
        .valueColumn(ColumnName.of("Fred"), SqlTypes.STRING)
        .valueColumn(ColumnNames.generatedColumnAlias(1), SqlTypes.STRING)
        .valueColumn(ColumnName.of("George"), SqlTypes.STRING)
        .build();

    // When:
    final ColumnName result = ColumnNames.nextGeneratedColumnAlias(schema);

    // Then:
    assertThat(result, is(ColumnName.of("KSQL_COL_2")));
  }
}