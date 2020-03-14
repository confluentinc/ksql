package io.confluent.ksql.function.udf.string;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.junit.Assert.*;

public class InitCapTest {
  private InitCap udf;

  @Before
  public void setUp() {
    udf = new InitCap();
  }

  @Test
  public void shouldHandleNull() {
    assertThat(udf.initcap(null), isEmptyOrNullString());
  }

  @Test
  public void shouldInitCap() {
    assertThat(udf.initcap("worD"), is("Word"));
    assertThat(udf.initcap("a"), is("A"));
    assertThat(udf.initcap("the Quick br0wn fOx"), is("The Quick Br0wn Fox"));
    assertThat(udf.initcap("spacing   should  be preserved"), is("Spacing   Should  Be Preserved"));
  }
}