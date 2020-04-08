package io.confluent.ksql.cli.console.table.builder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.cli.console.table.Table;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.QueryStateCount;
import io.confluent.ksql.rest.entity.RunningQuery;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueriesTableBuilderTest {

  private static final String STATE = "RUNNING:1";

  @Mock
  private QueryStateCount queryStateCount;
  
  @Before
  public void setup() {
    when(queryStateCount.toString()).thenReturn(STATE);
  }
  @Test
  public void shouldBuildQueriesTable() {
    // Given:
    final RunningQuery query = new RunningQuery(
        "EXAMPLE QUERY;",
        ImmutableSet.of("SINK"),
        ImmutableSet.of("SINK"),
        new QueryId("0"),
        queryStateCount
    );

    // When:
    final Table table = buildTableWithSingleQuery(query);

    // Then:
    assertThat(table.headers(), contains("Query ID", "Status", "Sink Name", "Sink Kafka Topic", "Query String"));
    assertThat(table.rows(), hasSize(1));
    assertThat(table.rows().get(0), contains("0", STATE, "SINK", "SINK", "EXAMPLE QUERY;"));
  }

  @Test
  public void shouldBuildQueriesTableWithNewlines() {
    // Given:
    final RunningQuery query = new RunningQuery(
        "CREATE STREAM S2 AS SELECT *\nFROM S1\nEMIT CHANGES;",
        ImmutableSet.of("S2"),
        ImmutableSet.of("S2"),
        new QueryId("CSAS_S2_0"),
        queryStateCount
    );


    // When:
    final Table table = buildTableWithSingleQuery(query);

    // Then:
    assertThat(table.headers(), contains("Query ID", "Status", "Sink Name", "Sink Kafka Topic", "Query String"));
    assertThat(table.rows(), hasSize(1));
    assertThat(table.rows().get(0), contains("CSAS_S2_0", STATE, "S2", "S2", "CREATE STREAM S2 AS SELECT * FROM S1 EMIT CHANGES;"));
  }

  private Table buildTableWithSingleQuery(RunningQuery query) {
    List<RunningQuery> queries = new ArrayList<>();
    queries.add(query);

    final Queries entity = new Queries(null, queries);

    QueriesTableBuilder builder = new QueriesTableBuilder();
    return builder.buildTable(entity);
  }
}
