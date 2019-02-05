package io.confluent.ksql.function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

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
    Object[] args = new Object[]{1, 1L, 1d, 1f};

    // Then:
    for (int i = 0; i < args.length; i++) {
      assertThat(UdfTemplate.coerce(args, int.class, i), equalTo(1));
      assertThat(UdfTemplate.coerce(args, Integer.class, i), equalTo(1));

      assertThat(UdfTemplate.coerce(args, long.class, i), equalTo(1L));
      assertThat(UdfTemplate.coerce(args, Long.class, i), equalTo(1L));

      assertThat(UdfTemplate.coerce(args, double.class, i), equalTo(1.0));
      assertThat(UdfTemplate.coerce(args, Double.class, i), equalTo(1.0));
    }
  }

  @Test
  public void testCoerceStrings() {
    // Given:
    Object[] args = new Object[]{"1", "1.2", "true"};

    // Then:
    assertThat(UdfTemplate.coerce(args, int.class, 0), equalTo(1));
    assertThat(UdfTemplate.coerce(args, Integer.class, 0), equalTo(1));

    assertThat(UdfTemplate.coerce(args, long.class, 0), equalTo(1L));
    assertThat(UdfTemplate.coerce(args, Long.class, 0), equalTo(1L));

    assertThat(UdfTemplate.coerce(args, double.class, 1), equalTo(1.2));
    assertThat(UdfTemplate.coerce(args, Double.class, 1), equalTo(1.2));

    assertThat(UdfTemplate.coerce(args, boolean.class, 2), is(true));
    assertThat(UdfTemplate.coerce(args, boolean.class, 2), is(true));
  }

  @Test
  public void testCoerceBoxedNull() {
    // Given:
    Object[] args = new Object[]{null};

    // Then:
    assertThat(UdfTemplate.coerce(args, Integer.class, 0), nullValue());
    assertThat(UdfTemplate.coerce(args, Long.class, 0), nullValue());
    assertThat(UdfTemplate.coerce(args, Double.class, 0), nullValue());
    assertThat(UdfTemplate.coerce(args, String.class, 0), nullValue());
    assertThat(UdfTemplate.coerce(args, Boolean.class, 0), nullValue());
    assertThat(UdfTemplate.coerce(args, Map.class, 0), nullValue());
    assertThat(UdfTemplate.coerce(args, List.class, 0), nullValue());
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
    assertThat(UdfTemplate.coerce(args, List.class, 0), equalTo(new ArrayList<>()));
    assertThat(UdfTemplate.coerce(args, Map.class, 1), equalTo(new HashMap<>()));
    assertThat(UdfTemplate.coerce(args, String.class, 2), equalTo(""));
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