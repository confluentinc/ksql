package io.confluent.ksql.function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

import com.google.common.primitives.Primitives;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.Test;

public class UdfTemplateTest {

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
    assertThat(UdfTemplate.coerce(args, Object[].class, 0), nullValue());
  }

  @Test
  public void testCoercePrimitiveFailsNull() {
    // Given:
    Object[] args = new Object[]{null};

    // When:
    final KsqlFunctionException e = assertThrows(
        KsqlFunctionException.class,
        () -> UdfTemplate.coerce(args, int.class, 0)
    );

    // Then:
    assertThat(e.getMessage(), containsString("from null to a primitive type"));
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
  public void shouldCoerceNullArray() {
    // Given:
    Object[] args = new Object[]{null};

    // Then:
    assertThat(UdfTemplate.coerce(args, Object[].class, 0), equalTo(null));
  }

  @Test
  public void shouldCoercePrimitiveArrays() {
    // Given:
    Object[] args = new Object[]{
        new int[]{1},
        new byte[]{1},
        new short[]{1},
        new float[]{1f},
        new double[]{1d},
        new boolean[]{true}
    };

    // Then:
    for (int i = 0; i < args.length; i++) {
      assertThat(UdfTemplate.coerce(args, args[i].getClass(), i), equalTo(args[i]));
    }
  }

  @Test
  public void shouldCoerceBoxedArrays() {
    // Given:
    Object[] args = new Object[]{
        new Integer[]{1},
        new Byte[]{1},
        new Short[]{1},
        new Float[]{1f},
        new Double[]{1d},
        new Boolean[]{true}
    };

    // Then:
    for (int i = 0; i < args.length; i++) {
      assertThat(UdfTemplate.coerce(args, args[i].getClass(), i), equalTo(args[i]));
    }
  }

  @Test
  public void shouldCoercePrimitiveArrayToBoxed() {
    // Given:
    Object[] args = new Object[]{
        new int[]{1},
        new byte[]{1},
        new short[]{1},
        new float[]{1f},
        new double[]{1d},
        new boolean[]{true}
    };

    // Then:
    for (int i = 0; i < args.length; i++) {
      final Class<?> boxed = Primitives.wrap(args[i].getClass().getComponentType());
      final Class<?> boxedArray = Array.newInstance(boxed, 0).getClass();
      assertThat(UdfTemplate.coerce(args, boxedArray, i), equalTo(args[i]));
    }
  }

  @Test
  public void shouldCoerceNumberConversionArray() {
    // Given:
    Object[] args = new Object[]{new int[]{1}};

    // Then:
    assertThat(UdfTemplate.coerce(args, double[].class, 0), equalTo(new double[]{1}));
  }

  @Test
  public void shouldCoerceArrayOfLists() {
    // Given:
    Object[] args = new Object[]{new List[]{new ArrayList()}};

    // Then:
    assertThat(UdfTemplate.coerce(args, List[].class, 0), equalTo(new List[]{new ArrayList<>()}));
  }

  @Test
  public void shouldCoerceArrayOfMaps() {
    // Given:
    Object[] args = new Object[]{new Map[]{new HashMap<>()}};

    // Then:
    assertThat(UdfTemplate.coerce(args, Map[].class, 0), equalTo(new Map[]{new HashMap<>()}));
  }

  @Test
  public void shouldNotCoerceNonArrayToArray() {
    // Given:
    Object[] args = new Object[]{"not an array"};

    // When:
    final KsqlFunctionException e = assertThrows(
        (KsqlFunctionException.class),
        () -> UdfTemplate.coerce(args, Object[].class, 0)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Couldn't coerce array"));
  }

  @Test
  public void testInvalidStringCoercion() {
    // Given:
    Object[] args = new Object[]{"not a number"};

    // When:
    final KsqlFunctionException e = assertThrows(
        KsqlFunctionException.class,
        () -> UdfTemplate.coerce(args, int.class, 0)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Couldn't coerce string"));
  }

  @Test
  public void testInvalidNumberCoercion() {
    // Given:
    Object[] args = new Object[]{1};

    // When:
    final KsqlFunctionException e = assertThrows(
        KsqlFunctionException.class,
        () -> UdfTemplate.coerce(args, Map.class, 0)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Couldn't coerce numeric"));
  }

  @Test
  public void testImpossibleCoercion() {
    // Given
    Object[] args = new Object[]{(Supplier) () -> null};

    // When:
    final KsqlFunctionException e = assertThrows(
        KsqlFunctionException.class,
        () -> UdfTemplate.coerce(args, int.class, 0)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Impossible to coerce"));
  }
}