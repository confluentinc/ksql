package io.confluent.ksql.function.udf.math;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;

public class SignTest {
  private Sign udf;

  @Before
  public void setUp() {
    udf = new Sign();
  }

  @Test
  public void shouldHandleNull() {
    assertThat(udf.sign((Integer)null), is(nullValue()));
    assertThat(udf.sign((Long)null), is(nullValue()));
    assertThat(udf.sign((Double)null), is(nullValue()));
  }

  @Test
  public void shouldHandleNegative() {
    assertThat(udf.sign(-10.0), is(-1.0));
  }

  @Test
  public void shouldHandleZero() {
    assertThat(udf.sign(0.0), is(0.0));
  }

  @Test
  public void shouldHandlePositive() {
    assertThat(udf.sign(1), is(1.0));
    assertThat(udf.sign(1L), is(1.0));
    assertThat(udf.sign(1.0), is(1.0));
  }
}