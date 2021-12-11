package io.confluent.ksql.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

public class PushOffsetVectorTest {

  @Test
  public void shouldDoBasics() {
    // Given:
    PushOffsetVector pushOffsetVector = new PushOffsetVector(ImmutableList.of(1L, 2L, 3L));

    // Then:
    assertThat(pushOffsetVector.getOffsets(), is(ImmutableList.of(1L, 2L, 3L)));
    assertThat(pushOffsetVector.copy().getOffsets(), is(ImmutableList.of(1L, 2L, 3L)));
    assertThat(pushOffsetVector, is(new PushOffsetVector(ImmutableList.of(1L, 2L, 3L))));
    assertThat(pushOffsetVector.getDenseRepresentation(), is(ImmutableList.of(1L, 2L, 3L)));
    assertThat(pushOffsetVector.getSparseRepresentation(),
        is(ImmutableMap.of(0, 1L, 1, 2L, 2, 3L)));
  }

  @Test
  public void shouldMerge() {
    // Given:
    PushOffsetVector pushOffsetVector1 = new PushOffsetVector(ImmutableList.of(1L, 2L, 3L));
    PushOffsetVector pushOffsetVector2 = new PushOffsetVector(ImmutableList.of(2L, 0L, 9L));

    // Then:
    assertThat(pushOffsetVector1.mergeCopy(pushOffsetVector2),
        is(new PushOffsetVector(ImmutableList.of(2L, 2L, 9L))));
    assertThat(pushOffsetVector2.mergeCopy(pushOffsetVector1),
        is(new PushOffsetVector(ImmutableList.of(2L, 2L, 9L))));
  }

  @Test
  public void shouldMerge_empty() {
    // Given:
    PushOffsetVector pushOffsetVector1 = new PushOffsetVector(ImmutableList.of(1L, 2L, 3L));
    PushOffsetVector pushOffsetVector2 = new PushOffsetVector();

    // Then:
    assertThat(pushOffsetVector1.mergeCopy(pushOffsetVector2),
        is(new PushOffsetVector(ImmutableList.of(1L, 2L, 3L))));
    assertThat(pushOffsetVector2.mergeCopy(pushOffsetVector1),
        is(new PushOffsetVector(ImmutableList.of(1L, 2L, 3L))));
  }

  @Test
  public void shouldBeLessThanOrEqual() {
    // Given:
    PushOffsetVector pushOffsetVector1 = new PushOffsetVector(ImmutableList.of(2L, 3L, 4L));

    // Then:
    assertThat(pushOffsetVector1.lessThanOrEqualTo(
        new PushOffsetVector(ImmutableList.of(2L, 3L, 4L))), is(true));
    assertThat(pushOffsetVector1.lessThanOrEqualTo(
        new PushOffsetVector(ImmutableList.of(3L, 3L, 4L))), is(true));
    assertThat(pushOffsetVector1.lessThanOrEqualTo(
        new PushOffsetVector(ImmutableList.of(3L, 4L, 4L))), is(true));
    assertThat(pushOffsetVector1.lessThanOrEqualTo(
        new PushOffsetVector(ImmutableList.of(3L, 4L, 5L))), is(true));
    assertThat(pushOffsetVector1.lessThanOrEqualTo(
        new PushOffsetVector(ImmutableList.of(1L, 3L, 4L))), is(false));
    assertThat(pushOffsetVector1.lessThanOrEqualTo(
        new PushOffsetVector(ImmutableList.of(1L, 2L, 4L))), is(false));
    assertThat(pushOffsetVector1.lessThanOrEqualTo(
        new PushOffsetVector(ImmutableList.of(1L, 2L, 3L))), is(false));
    // Special case
    assertThat(pushOffsetVector1.lessThanOrEqualTo(
        new PushOffsetVector(ImmutableList.of())), is(true));
  }
}
