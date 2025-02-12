package io.confluent.ksql.planner.plan;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.util.KsqlParserTestUtil;
import io.confluent.ksql.util.MetaStoreFixture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PullQueryRewriterTest {

  private MetaStore metaStore;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(mock(FunctionRegistry.class));
  }

  @Test
  public void shouldRewriteInPredicate() {
    assertRewrite("ORDERS", "ITEMID in ('a', 'b', 'c')",
        "((ORDERS.ITEMID = 'a') OR ((ORDERS.ITEMID = 'b') OR (ORDERS.ITEMID = 'c')))");
    assertRewrite("ORDERS", "ORDERID > 2 AND ITEMID in ('a', 'b', 'c')",
        "(((ORDERS.ORDERID > 2) AND (ORDERS.ITEMID = 'a')) OR (((ORDERS.ORDERID > 2) AND "
            + "(ORDERS.ITEMID = 'b')) OR ((ORDERS.ORDERID > 2) AND (ORDERS.ITEMID = 'c'))))");
    assertRewrite("ORDERS", "ORDERID > 2 OR ITEMID in ('a', 'b', 'c')",
        "((ORDERS.ORDERID > 2) OR ((ORDERS.ITEMID = 'a') OR ((ORDERS.ITEMID = 'b') OR "
            + "(ORDERS.ITEMID = 'c'))))");
  }

  @Test
  public void shouldRewriteBetweenPredicate() {
    assertRewrite("ORDERS", "ORDERID BETWEEN 2 AND 5",
            "((ORDERS.ORDERID >= 2) AND (ORDERS.ORDERID <= 5))");
    assertRewrite("ORDERS", "ORDERS.ITEMID BETWEEN 'a' AND 'b'",
            "((ORDERS.ITEMID >= 'a') AND (ORDERS.ITEMID <= 'b'))");
  }

  private void assertRewrite(final String table, final String expressionStr,
      final String expectedStr) {
    Expression expression = getWhereExpression(table, expressionStr);
    Expression converted = PullQueryRewriter.rewrite(expression);

    // When
    assertThat(converted.toString(), is(expectedStr));
  }

  private Expression getWhereExpression(final String table, String expression) {
    final Query statement = (Query) KsqlParserTestUtil
        .buildSingleAst("SELECT * FROM " + table + " WHERE " + expression + ";", metaStore)
        .getStatement();

    assertThat(statement.getWhere().isPresent(), is(true));
    return statement.getWhere().get();
  }
}
