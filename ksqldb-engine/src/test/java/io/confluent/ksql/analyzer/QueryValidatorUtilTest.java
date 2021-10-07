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
import io.confluent.ksql.util.KsqlConfig;
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
  @Mock
  private KsqlConfig ksqlConfig;


  @Test
  public void shouldThrowOnUserColumnsWithSameNameAsNewPseudoColumns() {
    // Given:
    givenAnalysisOfQueryWithUserColumnsWithSameNameAsNewPseudoColumns();

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> QueryValidatorUtil.validateNoUserColumnsWithSameNameAsNewPseudoColumns(analysis)
    );

    // Then:
    assertThat(e.getMessage(), containsString("You cannot query a stream or table that "
        + "has user columns with the same name as new pseudocolumns.\n"
        + "To allow for new queries on this source, downgrade your ksql "
        + "version to a release before the conflicting pseudocolumns "
        + "were introduced."));

  }

  public void givenAnalysisOfQueryWithUserColumnsWithSameNameAsNewPseudoColumns() {
    when(analysis.getAllDataSources()).thenReturn(ImmutableList.of(aliasedDataSource));
    when(aliasedDataSource.getDataSource()).thenReturn(dataSource);
    when(dataSource.getSchema()).thenReturn(logicalSchema);
    when(logicalSchema.value()).thenReturn(ImmutableList.of(column));
    when(column.name()).thenReturn(SystemColumns.ROWPARTITION_NAME);
    when(analysis.getKsqlConfig()).thenReturn(ksqlConfig);
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_ROWPARTITION_ROWOFFSET_ENABLED)).thenReturn(true);
  }

}
