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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.materialization.MaterializationInfo.Builder;
import io.confluent.ksql.execution.materialization.MaterializationInfo.TransformFactory;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
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
  private TransformFactory<KsqlTransformer<Object, GenericRow>> mapperFactory;

  @Before
  public void setUp() {

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
  public void shouldDropMetaColumnsFromMapSchema() {
    // Given:
    final Builder builder = MaterializationInfo
        .builder(STATE_STORE_NAME, SCHEMA_WITH_META);

    // When:
    final MaterializationInfo info = builder
        .map(mapperFactory, OTHER_SCHEMA_WITH_META, "stepName")
        .build();

    // Then:
    assertThat(info.getSchema(), is(OTHER_SCHEMA_WITH_META.withoutMetaColumns()));
  }
}