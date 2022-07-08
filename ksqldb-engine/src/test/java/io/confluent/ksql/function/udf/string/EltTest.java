package io.confluent.ksql.function.udf.string;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import org.junit.Test;

public class EltTest {

  private final Elt elt = new Elt();

  @Test
  public void shouldSelectFirstElementOfOne() {
    // When:
    final String el = elt.elt(1, "a");

    // Then:
    assertThat(el, equalTo("a"));
  }

  @Test
  public void shouldSelectFirstElementOfTwo() {
    // When:
    final String el = elt.elt(1, "a", "b");

    // Then:
    assertThat(el, equalTo("a"));
  }

  @Test
  public void shouldSelectSecondElementOfTwo() {
    // When:
    final String el = elt.elt(2, "a", "b");

    // Then:
    assertThat(el, equalTo("b"));
  }

  @Test
  public void shouldReturnNullIfNIsLessThanOne() {
    // When:
    final String el = elt.elt(0, "a", "b");

    // Then:
    assertThat(el, is(nullValue()));
  }

  @Test
  public void shouldReturnNullIfNIsGreaterThanLength() {
    // When:
    final String el = elt.elt(3, "a", "b");

    // Then:
    assertThat(el, is(nullValue()));
  }

  @Test
  public void shouldHandleNulls() {
    // When:
    final String el = elt.elt(2, "a", null);

    // Then:
    assertThat(el, is(nullValue()));
  }

  @Test
  public void shouldHandleNoArgs() {
    // When:
    final String el = elt.elt(2);

    // Then:
    assertThat(el, is(nullValue()));
  }

  @Test
  public void shouldHandleNullArgs() {
    // When:
    String[] array = null;
    final String el = elt.elt(2, array);

    // Then:
    assertThat(el, is(nullValue()));
  }
}