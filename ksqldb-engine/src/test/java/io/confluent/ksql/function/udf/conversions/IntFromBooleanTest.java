package io.confluent.ksql.function.udf.conversions;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class IntFromBooleanTest {
  private final IntFromBoolean udf = new IntFromBoolean();

  @Test
  public void shouldReturnNullOnNullBoolean() {
    assertThat(udf.intFromBoolean(null), is(nullValue()));
  }

  @Test
  public void shouldConvertTrueToInteger() {
    assertThat(udf.intFromBoolean(Boolean.TRUE), is(1));
  }

  @Test
  public void shouldConvertFalseToInteger() {
    assertThat(udf.intFromBoolean(Boolean.FALSE), is(0));
  }

}
