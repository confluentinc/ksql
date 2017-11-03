package io.confluent.ksql.parser.tree;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for class {@link SubqueryExpression}.
 *
 * @see SubqueryExpression
 */
public class SubqueryExpressionTest {

  @Test
  public void testEqualsReturningTrue() {
    SubqueryExpression subqueryExpressionOne = new SubqueryExpression(null);
    SubqueryExpression subqueryExpressionTwo = new SubqueryExpression(null);

    assertTrue(subqueryExpressionOne.equals(subqueryExpressionTwo));
    assertTrue(subqueryExpressionOne.equals(subqueryExpressionOne));
  }

  @Test
  public void testEqualsWithNull() {
    SubqueryExpression subqueryExpression = new SubqueryExpression(null);

    assertFalse(subqueryExpression.equals(null));
  }

  @Test
  public void testEqualsReturningFalse() {
    SubqueryExpression subqueryExpression = new SubqueryExpression(null);

    assertFalse(subqueryExpression.equals(new Object()));
  }
}