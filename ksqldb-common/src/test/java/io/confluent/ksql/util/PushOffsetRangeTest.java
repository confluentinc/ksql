package io.confluent.ksql.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import java.util.Optional;
import org.junit.Test;

public class PushOffsetRangeTest {


  @Test
  public void shouldHaveBothStartEnd() {
    // Given:
    PushOffsetRange offsetRange = new PushOffsetRange(
        Optional.of(new PushOffsetVector(ImmutableList.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L))),
        new PushOffsetVector(ImmutableList.of(10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L)));

    // That:
    assertThat(
        offsetRange.serialize(),
        is("eyJzIjp7Im8iOlsxLDIsMyw0LDUsNiw3LDhdfSwiZSI6eyJvIjpbMTAsOSw4LDcsNiw1LDQsM119LCJ2IjowfQ"
            + "==")
    );

    PushOffsetRange decoded = PushOffsetRange.deserialize(
        "eyJzIjp7Im8iOlsxLDIsMyw0LDUsNiw3LDhdfSwiZSI6eyJvIjpbMTAsOSw4LDcsNiw1LDQsM119LCJ2IjowfQ"
            + "==");
    assertThat(decoded.getVersion(), is(0));
    assertThat(decoded.getStartOffsets().isPresent(), is(true));
    assertThat(decoded.getStartOffsets().get().getDenseRepresentation(),
        is(ImmutableList.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L)));
    assertThat(decoded.getEndOffsets().getDenseRepresentation(),
        is(ImmutableList.of(10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L)));
  }

  @Test
  public void shouldHaveJustEnd() {
    PushOffsetRange offsetRange = new PushOffsetRange(
        Optional.empty(),
        new PushOffsetVector(ImmutableList.of(10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L)));
    assertThat(
        offsetRange.serialize(),
        is("eyJzIjpudWxsLCJlIjp7Im8iOlsxMCw5LDgsNyw2LDUsNCwzXX0sInYiOjB9")
    );

    PushOffsetRange decoded = PushOffsetRange.deserialize(
        "eyJzIjpudWxsLCJlIjp7Im8iOlsxMCw5LDgsNyw2LDUsNCwzXX0sInYiOjB9");
    assertThat(decoded.getVersion(), is(0));
    assertThat(decoded.getStartOffsets().isPresent(), is(false));
    assertThat(decoded.getEndOffsets().getDenseRepresentation(),
        is(ImmutableList.of(10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L)));
  }
}
