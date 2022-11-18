package io.confluent.ksql.function.udf.math;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.math.BigDecimal;
import org.junit.Before;
import org.junit.Test;

public class GreatestTest {

  private Greatest udf;

  @Before
  public void setUp() {
    udf = new Greatest();
  }

  @Test
  public void shouldWorkWithoutImplicitCasting(){
    assertThat(udf.greatest(0, 1, -1, 2, -2), is(2));
    assertThat(udf.greatest(0L, 1L, -1L, 2L, -2L), is(2L));
    assertThat(udf.greatest(0D, .1, -.1, .2, -.2), is(.2));
    assertThat(udf.greatest(new BigDecimal("0"), new BigDecimal(".1"), new BigDecimal("-.1"), new BigDecimal(".2"), new BigDecimal("-.2")), is(new BigDecimal(".2")));
    assertThat(udf.greatest("apple", "banana", "bzzz"), is("bzzz"));
  }

  @Test
  public void shouldHandleAllNullColumns(){
    assertThat(udf.greatest((Integer) null, null, null), is(nullValue()));
    assertThat(udf.greatest((Double) null, null, null), is(nullValue()));
    assertThat(udf.greatest((Long) null, null, null), is(nullValue()));
    assertThat(udf.greatest((BigDecimal) null, null, null), is(nullValue()));
    assertThat(udf.greatest((String) null, null, null), is(nullValue()));
  }

  @Test
  public void shouldHandleNullArrays(){
    assertThat(udf.greatest((Integer) null, null), is(nullValue()));
    assertThat(udf.greatest((Double) null, null), is(nullValue()));
    assertThat(udf.greatest((Long) null, null), is(nullValue()));
    assertThat(udf.greatest((BigDecimal) null, null), is(nullValue()));
    assertThat(udf.greatest((String) null, null), is(nullValue()));
  }

  @Test
  public void shouldHandleSomeNullColumns(){
    assertThat(udf.greatest(null, 27, null, 39, -49, -11, 68, 32, null, 101), is(101));
    assertThat(udf.greatest(null, null, 39D, -49.01, -11.98, 68.1, .32, null, 101D), is(101D));
    assertThat(udf.greatest(null, 272038202439L, null, 39L, -4923740932490L, -11L, 68L, 32L, null, 101L), is(272038202439L));
    assertThat(udf.greatest(null, new BigDecimal("27"), null, new BigDecimal("-49")), is(new BigDecimal("27")));
    assertThat(udf.greatest(null, "apple", null, "banana", "kumquat", "aardvark", null), is("kumquat"));
  }

}