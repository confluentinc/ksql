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
package io.confluent.ksql.analyzer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.util.KsqlException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueryValidatorUtilTest {

  @Mock
  private Analysis analysis;
  @Mock
  private Analysis.AliasedDataSource aliasedDataSource;
  @Mock
  private DataSource dataSource;
  @Mock
  private LogicalSchema logicalSchema;
  @Mock
  private Column column;


  @Test
  public void shouldThrowOnUserColumnsWithSameNameAsPseudoColumn() {
    // Given:
    givenAnalysisOfQueryWithUserColumnsWithSameNameAsPseudoColumn();

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> QueryValidatorUtil.validateNoUserColumnsWithSameNameAsPseudoColumns(analysis)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Your stream/table has columns with the "
        + "same name as newly introduced pseudocolumns in "
        + "ksqlDB, and cannot be queried as a result. The conflicting names are: `ROWPARTITION`."));

  }

  private void givenAnalysisOfQueryWithUserColumnsWithSameNameAsPseudoColumn() {
    when(analysis.getAllDataSources()).thenReturn(ImmutableList.of(aliasedDataSource));
    when(aliasedDataSource.getDataSource()).thenReturn(dataSource);
    when(dataSource.getSchema()).thenReturn(logicalSchema);
    when(logicalSchema.value()).thenReturn(ImmutableList.of(column));
    when(column.name()).thenReturn(SystemColumns.ROWPARTITION_NAME);
  }

}
