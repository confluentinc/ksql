package io.confluent.ksql.parser.tree;

import org.junit.Test;

import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Unit tests for class {@link Intersect}.
 *
 * @see Intersect
 */
public class IntersectTest {

  @Test
  public void testFailsToCreateIntersectTakingNullAsFirstArgument() {
    try {
      new Intersect(null, false);
      fail("Expecting exception: NullPointerException");
    } catch (NullPointerException e) {
      assertEquals(Objects.class.getName(), e.getStackTrace()[0].getClassName());
    }
  }
}