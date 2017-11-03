package io.confluent.ksql.parser.tree;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for class {@link Cast}.
 *
 * @see Cast
 */
public class CastTest {

  @Test
  public void testEqualsReturningTrue() {
    ArithmeticBinaryExpression.Type arithmeticBinaryExpression_Type = ArithmeticBinaryExpression.Type.MODULUS;
    ArithmeticBinaryExpression arithmeticBinaryExpression = new ArithmeticBinaryExpression(arithmeticBinaryExpression_Type, null, null);
    Cast castOne = new Cast(arithmeticBinaryExpression, "pF(3kL7/byV6r0*W7@0", true);
    NodeLocation nodeLocation = new NodeLocation((-1862), (-1862));
    Cast castTwo = new Cast(nodeLocation, arithmeticBinaryExpression, "pF(3kL7/byV6r0*W7@0", true);

    assertTrue(castOne.equals(castOne));
    assertTrue(castOne.equals(castTwo));
  }

  @Test
  public void testEqualsReturningFalse() {
    ArithmeticBinaryExpression.Type arithmeticBinaryExpression_Type = ArithmeticBinaryExpression.Type.MODULUS;
    ArithmeticBinaryExpression arithmeticBinaryExpression = new ArithmeticBinaryExpression(arithmeticBinaryExpression_Type, null, null);
    NodeLocation nodeLocation = new NodeLocation((-1862), (-1862));
    Cast castOne = new Cast(nodeLocation, arithmeticBinaryExpression, "^tgVV", true);
    Cast castTwo = new Cast(arithmeticBinaryExpression, "^tgVV");

    assertFalse(castOne.equals(castTwo));
    assertFalse(castOne.equals(null));
    assertFalse(castOne.equals(new Object()));
  }
}