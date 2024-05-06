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
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LogicRewriterTest {

  private MetaStore metaStore;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(mock(FunctionRegistry.class));
  }

  @Test
  public void shouldPropagateNots() {
    assertNeg("ORDERS", "ORDERUNITS > 1", "(ORDERS.ORDERUNITS > 1)");
    assertNeg("ORDERS", "NOT ORDERUNITS > 1", "(NOT (ORDERS.ORDERUNITS > 1))");
    assertNeg("TEST3", "NOT COL4", "(NOT TEST3.COL4)");
    assertNeg("TEST3", "NOT NOT COL4", "TEST3.COL4");
    assertNeg("TEST3", "NOT NOT NOT COL4", "(NOT TEST3.COL4)");
    assertNeg("TEST3", "NOT true", "(NOT true)");
    assertNeg("TEST3", "NOT true", "(NOT true)");
    assertNeg("ORDERS", "ORDERUNITS > 1 AND ITEMID = 'a'",
        "((ORDERS.ORDERUNITS > 1) AND (ORDERS.ITEMID = 'a'))");
    assertNeg("ORDERS", "NOT ORDERUNITS > 1 AND ITEMID = 'a'",
        "((NOT (ORDERS.ORDERUNITS > 1)) AND (ORDERS.ITEMID = 'a'))");
    assertNeg("ORDERS", "NOT (ORDERUNITS > 1 AND ITEMID = 'a')",
        "((NOT (ORDERS.ORDERUNITS > 1)) OR (NOT (ORDERS.ITEMID = 'a')))");
    assertNeg("ORDERS", "NOT (ORDERUNITS + 2 > 1 AND ITEMID = 'a' + 'b')",
        "((NOT ((ORDERS.ORDERUNITS + 2) > 1)) OR (NOT (ORDERS.ITEMID = ('a' + 'b'))))");
    assertNeg("ORDERS", "NOT (ORDERUNITS > 1 OR ITEMID = 'a')",
        "((NOT (ORDERS.ORDERUNITS > 1)) AND (NOT (ORDERS.ITEMID = 'a')))");
    assertNeg("TEST5", "NOT (A AND B)", "((NOT TEST5.A) OR (NOT TEST5.B))");
    assertNeg("TEST5", "NOT (A OR B)", "((NOT TEST5.A) AND (NOT TEST5.B))");
  }

  @Test
  public void shouldBeCNF() {
    assertCNF("TEST5", "A", "TEST5.A");
    assertCNF("TEST5", "A AND B", "(TEST5.A AND TEST5.B)");
    assertCNF("TEST5", "A OR B", "(TEST5.A OR TEST5.B)");
    assertCNF("TEST5", "(A OR B) AND (C OR D)",
        "((TEST5.A OR TEST5.B) AND (TEST5.C OR TEST5.D))");
    assertCNF("TEST5", "(A AND B) OR (C AND D)",
        "(((TEST5.A OR TEST5.C) AND (TEST5.A OR TEST5.D)) AND "
            + "((TEST5.B OR TEST5.C) AND (TEST5.B OR TEST5.D)))");
    assertCNF("TEST5", "(A AND B) OR (C AND D) OR (E AND F)",
        "(((((TEST5.A OR TEST5.C) OR TEST5.E) AND ((TEST5.A OR TEST5.C) OR TEST5.F)) AND "
            + "(((TEST5.A OR TEST5.D) OR TEST5.E) AND ((TEST5.A OR TEST5.D) OR TEST5.F))) AND "
            + "((((TEST5.B OR TEST5.C) OR TEST5.E) AND ((TEST5.B OR TEST5.C) OR TEST5.F)) AND "
            + "(((TEST5.B OR TEST5.D) OR TEST5.E) AND ((TEST5.B OR TEST5.D) OR TEST5.F))))");

    assertCNF("TEST5", "(NOT A AND B) OR (C AND NOT D)",
        "((((NOT TEST5.A) OR TEST5.C) AND ((NOT TEST5.A) OR (NOT TEST5.D))) AND "
            + "((TEST5.B OR TEST5.C) AND (TEST5.B OR (NOT TEST5.D))))");
    assertCNF("TEST5", "NOT (A AND B) OR NOT (C AND D)",
        "(((NOT TEST5.A) OR (NOT TEST5.B)) OR ((NOT TEST5.C) OR (NOT TEST5.D)))");
  }

  @Test
  public void shouldBeDNF() {
    assertDNF("TEST5", "A", "TEST5.A");
    assertDNF("TEST5", "A AND B", "(TEST5.A AND TEST5.B)");
    assertDNF("TEST5", "A OR B", "(TEST5.A OR TEST5.B)");
    assertDNF("TEST5", "(A OR B) AND (C OR D)",
        "(((TEST5.A AND TEST5.C) OR (TEST5.A AND TEST5.D)) OR "
            + "((TEST5.B AND TEST5.C) OR (TEST5.B AND TEST5.D)))");
    assertDNF("TEST5", "(A AND B) OR (C AND D)",
        "((TEST5.A AND TEST5.B) OR (TEST5.C AND TEST5.D))");
    assertDNF("TEST5", "(A OR B) AND (C OR D) AND (E OR F)",
        "(((((TEST5.A AND TEST5.C) AND TEST5.E) OR ((TEST5.A AND TEST5.C) AND TEST5.F)) OR "
            + "(((TEST5.A AND TEST5.D) AND TEST5.E) OR ((TEST5.A AND TEST5.D) AND TEST5.F))) OR "
            + "((((TEST5.B AND TEST5.C) AND TEST5.E) OR ((TEST5.B AND TEST5.C) AND TEST5.F)) OR "
            + "(((TEST5.B AND TEST5.D) AND TEST5.E) OR ((TEST5.B AND TEST5.D) AND TEST5.F))))");

    assertDNF("TEST5", "(NOT A OR B) AND (C OR NOT D)",
        "((((NOT TEST5.A) AND TEST5.C) OR ((NOT TEST5.A) AND (NOT TEST5.D))) OR "
            + "((TEST5.B AND TEST5.C) OR (TEST5.B AND (NOT TEST5.D))))");
    assertDNF("TEST5", "NOT (A OR B) AND NOT (C OR D)",
        "(((NOT TEST5.A) AND (NOT TEST5.B)) AND ((NOT TEST5.C) AND (NOT TEST5.D)))");
  }

  @Test
  public void shouldExtractDisjuncts() {
    assertExtractDisjuncts("TEST5", "A", "TEST5.A");
    assertExtractDisjuncts("TEST5", "A AND B", "(TEST5.A AND TEST5.B)");
    assertExtractDisjuncts("TEST5", "A OR B", "TEST5.A", "TEST5.B");
    assertExtractDisjuncts("TEST5", "(A OR B) AND (C OR D)", "(TEST5.A AND TEST5.C)",
        "(TEST5.A AND TEST5.D)", "(TEST5.B AND TEST5.C)", "(TEST5.B AND TEST5.D)");

    assertExtractDisjuncts("ORDERS", "ORDERUNITS > 1", "(ORDERS.ORDERUNITS > 1)");
    assertExtractDisjuncts("ORDERS", "ORDERUNITS > 1 AND ITEMID = 'a'",
        "((ORDERS.ORDERUNITS > 1) AND (ORDERS.ITEMID = 'a'))");
    assertExtractDisjuncts("ORDERS", "NOT (ORDERUNITS > 1 AND ITEMID = 'a')",
        "(NOT (ORDERS.ORDERUNITS > 1))", "(NOT (ORDERS.ITEMID = 'a'))");
  }

  private void assertNeg(final String table, final String expressionStr, final String expectedStr) {
    Expression expression = getWhereExpression(table, expressionStr);
    Expression converted = LogicRewriter.rewriteNegations(expression);

    // When
    assertThat(converted.toString(), is(expectedStr));
  }

  private void assertCNF(final String table, final String expressionStr, final String expectedStr) {
    Expression expression = getWhereExpression(table, expressionStr);
    Expression converted = LogicRewriter.rewriteCNF(expression);

    // When
    assertThat(converted.toString(), is(expectedStr));
  }

  private void assertDNF(final String table, final String expressionStr, final String expectedStr) {
    Expression expression = getWhereExpression(table, expressionStr);
    Expression converted = LogicRewriter.rewriteDNF(expression);

    // When
    assertThat(converted.toString(), is(expectedStr));
  }

  private void assertExtractDisjuncts(final String table, final String expressionStr,
      final String... expectedStrs) {
    Expression expression = getWhereExpression(table, expressionStr);
    List<Expression> disjuncts = LogicRewriter.extractDisjuncts(expression);

    assertThat(disjuncts.size(), is(expectedStrs.length));

    // When
    int i = 0;
    for (Expression e : disjuncts) {
      assertThat(e.toString(), is(expectedStrs[i++]));
    }
  }

  private Expression getWhereExpression(final String table, String expression) {
    final Query statement = (Query) KsqlParserTestUtil
        .buildSingleAst("SELECT * FROM " + table + " WHERE " + expression + ";", metaStore)
        .getStatement();

    assertThat(statement.getWhere().isPresent(), is(true));
    return statement.getWhere().get();
  }
}
