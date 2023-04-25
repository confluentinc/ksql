package io.confluent.ksql.function.udf.nulls;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class NullIfTest {

  private NullIf udf;

  @Before
  public void setUp() {
    udf = new NullIf();
  }

  @Test
  public void shouldReturnNullIfBothValuesAreNulls() {
    assertThat(udf.nullIf(null, null), is(nullValue()));
  }

  @Test
  public void shouldReturnNullIfValue1IsNull() {
    assertThat(udf.nullIf(null, "a"), is(nullValue()));
  }

  @Test
  public void shouldReturnNullIfBothValuesAreEqual() {
    assertThat(udf.nullIf("a", "a"), is(nullValue()));
  }

  @Test
  public void shouldReturnValue1IfBothValuesAreNonEqual() {
    assertThat(udf.nullIf("a", "b"), is("a"));
  }

  @Test
  public void shouldReturnValue1IfValue1IsNotNullAndValue2IsNull(){
    assertThat(udf.nullIf("a", null), is("a"));
  }

  @Test
  public void shouldReturnValue1CaseSensitive(){
    assertThat(udf.nullIf("a", "A"), is("a"));
  }
}