package io.confluent.ksql.cli.console.table.builder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.cli.console.table.Table;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.QueryStatusCount;
import io.confluent.ksql.rest.entity.RunningQuery;
import java.util.ArrayList;
import java.util.List;

import io.confluent.ksql.util.KsqlConstants;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueriesTableBuilderTest {

  private static final String STATUS = "RUNNING:1";

  @Mock
  private QueryStatusCount queryStatusCount;
  
  @Before
  public void setup() {
    when(queryStatusCount.toString()).thenReturn(STATUS);
  }
  @Test
  public void shouldBuildQueriesTable() {
    // Given:
    final String exampleQuery = "select * from test_stream emit changes";
    final RunningQuery query = new RunningQuery(
            exampleQuery,
        ImmutableSet.of("SINK"),
        ImmutableSet.of("SINK"),
        new QueryId("0"),
        queryStatusCount,
        KsqlConstants.KsqlQueryType.TRANSIENT);

    // When:
    final Table table = buildTableWithSingleQuery(query);

    // Then:
    assertThat(table.headers(), contains("Query ID", "Query Type", "Status", "Sink Name", "Sink Kafka Topic", "Query String"));
    assertThat(table.rows(), hasSize(1));
    assertThat(table.rows().get(0), contains("0", "TRANSIENT", STATUS, "SINK", "SINK", exampleQuery));
  }

  @Test
  public void shouldBuildQueriesTableWithNewlines() {
    // Given:
    final RunningQuery query = new RunningQuery(
        "CREATE STREAM S2 AS SELECT *\nFROM S1\nEMIT CHANGES;",
        ImmutableSet.of("S2"),
        ImmutableSet.of("S2"),
        new QueryId("CSAS_S2_0"),
        queryStatusCount,
        KsqlConstants.KsqlQueryType.PUSH);


    // When:
    final Table table = buildTableWithSingleQuery(query);

    // Then:
    assertThat(table.headers(), contains("Query ID", "Query Type", "Status", "Sink Name", "Sink Kafka Topic", "Query String"));
    assertThat(table.rows(), hasSize(1));
    assertThat(table.rows().get(0), contains("CSAS_S2_0", "PUSH", STATUS, "S2", "S2", "CREATE STREAM S2 AS SELECT * FROM S1 EMIT CHANGES;"));
  }

  private Table buildTableWithSingleQuery(RunningQuery query) {
    List<RunningQuery> queries = new ArrayList<>();
    queries.add(query);

    final Queries entity = new Queries(null, queries);

    QueriesTableBuilder builder = new QueriesTableBuilder();
    return builder.buildTable(entity);
  }
}
