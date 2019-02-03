package io.confluent.ksql.function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class UdfTemplateTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testCoerceNumbers() {
    // Given:
    Object[] args = new Object[]{1, 1d, 1f};

    // Then:
    for (int i = 0; i < args.length; i++) {
      assertEquals(1, (int) UdfTemplate.coerce(args, int.class, i));
      assertEquals(1, (int) UdfTemplate.coerce(args, Integer.class, i));
      assertEquals((Integer) 1, UdfTemplate.coerce(args, int.class, i));
      assertEquals((Integer) 1, UdfTemplate.coerce(args, Integer.class, i));

      assertEquals(1, (long) UdfTemplate.coerce(args, long.class, i));
      assertEquals(1, (long) UdfTemplate.coerce(args, Long.class, i));
      assertEquals((Long) 1L, UdfTemplate.coerce(args, long.class, i));
      assertEquals((Long) 1L, UdfTemplate.coerce(args, Long.class, i));

      assertEquals(1d, UdfTemplate.coerce(args, double.class, i), 0.1);
      assertEquals(1d, UdfTemplate.coerce(args, Double.class, i), 0.1);
      assertEquals(1d, UdfTemplate.coerce(args, double.class, i), 0.1);
      assertEquals(1d, UdfTemplate.coerce(args, Double.class, i), 0.1);
    }
  }

  @Test
  public void testCoerceStrings() {
    // Given:
    Object[] args = new Object[]{"1", "1.2", "true"};

    // Then:
    assertEquals(1, (int) UdfTemplate.coerce(args, int.class, 0));
    assertEquals((Integer) 1, UdfTemplate.coerce(args, Integer.class, 0));
    assertEquals(1, (long) UdfTemplate.coerce(args, long.class, 0));
    assertEquals((Long) 1L, UdfTemplate.coerce(args, Long.class, 0));

    assertEquals(1.2d, (double) UdfTemplate.coerce(args, double.class, 1), .1);
    assertEquals((Double) 1.2, UdfTemplate.coerce(args, Double.class, 1), .1);

    assertTrue(UdfTemplate.coerce(args, boolean.class, 2));
    assertTrue(UdfTemplate.coerce(args, boolean.class, 2));
  }

  @Test
  public void testCoerceBoxedNull() {
    // Given:
    Object[] args = new Object[]{null};

    // Then:
    assertNull(UdfTemplate.coerce(args, Integer.class, 0));
    assertNull(UdfTemplate.coerce(args, Long.class, 0));
    assertNull(UdfTemplate.coerce(args, Double.class, 0));
    assertNull(UdfTemplate.coerce(args, String.class, 0));
    assertNull(UdfTemplate.coerce(args, Boolean.class, 0));
    assertNull(UdfTemplate.coerce(args, Map.class, 0));
    assertNull(UdfTemplate.coerce(args, List.class, 0));
  }

  @Test
  public void testCoercePrimitiveFailsNull() {
    // Given:
    Object[] args = new Object[]{null};

    // Then:
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("from null to a primitive type");

    // When:
    UdfTemplate.coerce(args, int.class, 0);
  }

  @Test
  public void testCoerceObjects() {
    // Given:
    Object[] args = new Object[]{new ArrayList<>(), new HashMap<>(), ""};

    // Then:
    assertEquals(new ArrayList<>(), UdfTemplate.coerce(args, List.class, 0));
    assertEquals(new HashMap<>(), UdfTemplate.coerce(args, Map.class, 1));
    assertEquals("", UdfTemplate.coerce(args, String.class, 2));
  }

  @Test
  public void testInvalidStringCoercion() {
    // Given:
    Object[] args = new Object[]{"not a number"};

    // Then:
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("Couldn't coerce string");

    // When:
    UdfTemplate.coerce(args, int.class, 0);
  }

  @Test
  public void testInvalidNumberCoercion() {
    // Given:
    Object[] args = new Object[]{1};

    // Then:
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("Couldn't coerce numeric");

    // When:
    UdfTemplate.coerce(args, Map.class, 0);
  }

  @Test
  public void testImpossibleCoercion() {
    // Given
    Object[] args = new Object[]{(Supplier) () -> null};

    // Then:
    expectedException.expect(KsqlFunctionException.class);
    expectedException.expectMessage("Impossible to coerce");

    // When:
    UdfTemplate.coerce(args, int.class, 0);
  }

}