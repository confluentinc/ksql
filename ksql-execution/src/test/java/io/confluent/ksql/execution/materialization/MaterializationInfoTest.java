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

package io.confluent.ksql.execution.materialization;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import io.confluent.ksql.execution.materialization.MaterializationInfo.Builder;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MaterializationInfoTest {

  private static final String STATE_STORE_NAME = "Bob";
  private static final ColumnName ID = ColumnName.of("ID");
  private static final ColumnName NAME = ColumnName.of("NAME");
  private static final ColumnName AGE = ColumnName.of("AGE");

  private static final LogicalSchema SCHEMA_WITH_META = LogicalSchema.builder()
      .valueColumn(NAME, SqlTypes.STRING)
      .keyColumn(ID, SqlTypes.BIGINT)
      .build();

  private static final LogicalSchema OTHER_SCHEMA_WITH_META = LogicalSchema.builder()
      .valueColumn(NAME, SqlTypes.STRING)
      .keyColumn(ID, SqlTypes.BIGINT)
      .valueColumn(AGE, SqlTypes.INTEGER)
      .build();

  @Mock
  private AggregatesInfo aggregateInfo;
  @Mock
  private List<SelectExpression> selectExpressions;

  @Before
  public void setUp() {
    when(aggregateInfo.valueColumnCount())
        .thenReturn(OTHER_SCHEMA_WITH_META.value().size());

    when(selectExpressions.size())
        .thenReturn(OTHER_SCHEMA_WITH_META.value().size());
  }

  @Test
  public void shouldDropMetaColumnsFromStateStoreSchema() {
    // When:
    final MaterializationInfo info = MaterializationInfo
        .builder(STATE_STORE_NAME, SCHEMA_WITH_META)
        .build();

    // Then:
    assertThat(info.getStateStoreSchema(), is(SCHEMA_WITH_META.withoutMetaColumns()));
  }

  @Test
  public void shouldDropMetaColumnsFromResultSchema() {
    // When:
    final MaterializationInfo info = MaterializationInfo
        .builder(STATE_STORE_NAME, SCHEMA_WITH_META)
        .build();

    // Then:
    assertThat(info.getSchema(), is(SCHEMA_WITH_META.withoutMetaColumns()));
  }

  @Test
  public void shouldDropMetaColumnsFromAggregateSchema() {
    // Given:
    final Builder builder = MaterializationInfo
        .builder(STATE_STORE_NAME, SCHEMA_WITH_META);

    // When:
    final MaterializationInfo info = builder
        .mapAggregates(aggregateInfo, OTHER_SCHEMA_WITH_META)
        .build();

    // Then:
    assertThat(info.getSchema(), is(OTHER_SCHEMA_WITH_META.withoutMetaColumns()));
  }

  @Test
  public void shouldDropMetaColumnsFromProjectionSchema() {
    // Given:
    final Builder builder = MaterializationInfo
        .builder(STATE_STORE_NAME, SCHEMA_WITH_META);

    // When:
    final MaterializationInfo info = builder
        .project(selectExpressions, OTHER_SCHEMA_WITH_META)
        .build();

    // Then:
    assertThat(info.getSchema(), is(OTHER_SCHEMA_WITH_META.withoutMetaColumns()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnAggregateSchemaValueColumnCountMismatch() {
    // Given:
    final Builder builder = MaterializationInfo
        .builder(STATE_STORE_NAME, SCHEMA_WITH_META);

    when(aggregateInfo.valueColumnCount()).thenReturn(1);

    // When:
    builder.mapAggregates(aggregateInfo, OTHER_SCHEMA_WITH_META);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnProjectionSchemaValueColumnCountMismatch() {
    // Given:
    final Builder builder = MaterializationInfo
        .builder(STATE_STORE_NAME, SCHEMA_WITH_META);

    when(selectExpressions.size()).thenReturn(1);

    // When:
    builder.project(selectExpressions, OTHER_SCHEMA_WITH_META);
  }
}