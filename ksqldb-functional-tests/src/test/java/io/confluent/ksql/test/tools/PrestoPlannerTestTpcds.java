package io.confluent.ksql.test.tools;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.testing.TestingConnectorContext;
import com.facebook.presto.tpcds.TpcdsConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import net.hydromatic.tpcds.query.Query;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class PrestoPlannerTestTpcds {
  private static PrestoPlanner prestoPlanner;
  private final String sql;

  @BeforeClass
  public static void beforeClass() {

    final Connector tpcds =
        new TpcdsConnectorFactory()
            .create(
                "catalog",
                Collections.emptyMap(),
                new TestingConnectorContext()
            );
    final String schemaName = "tiny";
    prestoPlanner = new PrestoPlanner(schemaName, tpcds);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    final Builder<Object[]> builder = ImmutableList.builder();
    for (final Query value : Query.values()) {
      final String query = value.sql(new Random(0));
      builder.add(new Object[]{"TPCDS Query " + value.id, query});
    }
    return builder.build();
  }

  public PrestoPlannerTestTpcds(final String name, final String sql) {
    this.sql = sql;
  }

  @Test
  public void logicalPlan() {
    System.out.println(sql);
    final Plan prestoPlan = prestoPlanner.logicalPlan(
        sql
    );
  }
}
