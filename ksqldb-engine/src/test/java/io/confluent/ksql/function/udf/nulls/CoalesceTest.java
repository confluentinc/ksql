package io.confluent.ksql.function.udf.nulls;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

public class CoalesceTest {

  private Coalesce udf;

  @Before
  public void setUp() {
    udf = new Coalesce();
  }

  @Test
  public void shouldReturnNullForNullOnly() {
    assertThat(udf.coalesce(null), is(nullValue()));
  }

  @Test
  public void shouldReturnNullForNullOthers() {
    assertThat(udf.coalesce(null, (String[]) null), is(nullValue()));
  }

  @Test
  public void shouldReturnNullForEmptyOthers() {
    assertThat(udf.coalesce(null, new Double[]{}), is(nullValue()));
  }

  @Test
  public void shouldReturnFirstNonNullEntity() {
    assertThat(udf.coalesce(1, 2, 3), is(1));
    assertThat(udf.coalesce(null, "a", "b", "c", "d"), is("a"));
    assertThat(udf.coalesce(null, ImmutableList.of(), null), is(ImmutableList.of()));
    assertThat(udf.coalesce(null, null, 1.0), is(1.0));
  }
}