package io.confluent.ksql.test.tools;

import static com.facebook.presto.tpch.TpchConnectorFactory.TPCH_COLUMN_NAMING_PROPERTY;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.testing.TestingConnectorContext;
import com.facebook.presto.tpcds.TpcdsConnectorFactory;
import com.facebook.presto.tpch.ColumnNaming;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import net.hydromatic.tpcds.query.Query;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class PrestoPlannerTestTpch {
  private static PrestoPlanner prestoPlanner;
  private final String sql;

  @BeforeClass
  public static void beforeClass() {

    final ImmutableMap<String, String> of = ImmutableMap.of(
        TPCH_COLUMN_NAMING_PROPERTY, ColumnNaming.STANDARD.name()
    );
    final Connector connector =
        new TpchConnectorFactory()
            .create(
                "catalog",
                of,
                new TestingConnectorContext()
            );
    final String schemaName = "sf1";
    prestoPlanner = new PrestoPlanner(schemaName, connector);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    final Builder<Object[]> builder = ImmutableList.builder();
    for (int i = 0; i < CalciteTPCHQueries.QUERIES.size(); i++) {
      final String query = CalciteTPCHQueries.QUERIES.get(i);
      final String replace =
          query
              .replace("tpch.", "")
//              .replace("l_","")
//              .replace("p_","")
//              .replace("s_","")
//              .replace("n_","")
//              .replace("r_","")
          ;
      builder.add(new Object[]{"TPCH Query " + i, replace});
    }
    return builder.build();
  }

  public PrestoPlannerTestTpch(final String name, final String sql) {
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
