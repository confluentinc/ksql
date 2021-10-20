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
        offsetRange.token(),
        is("eyJzdGFydE9mZnNldHMiOnsib2Zmc2V0cyI6WzEsMiwzLDQsNSw2LDcsOF19LCJlbmRPZmZzZXRzIjp7Im9mZnN"
            + "ldHMiOlsxMCw5LDgsNyw2LDUsNCwzXX19")
    );

    PushOffsetRange decoded = PushOffsetRange.fromToken(
        "eyJzdGFydE9mZnNldHMiOnsib2Zmc2V0cyI6WzEsMiwzLDQsNSw2LDcsOF19LCJlbmRPZmZzZXRzIjp7Im9mZnN"
            + "ldHMiOlsxMCw5LDgsNyw2LDUsNCwzXX19");
    assertThat(decoded.getStartOffsets().isPresent(), is(true));
    assertThat(decoded.getStartOffsets().get().getOffsets(),
        is(ImmutableList.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L)));
    assertThat(decoded.getEndOffsets().getOffsets(),
        is(ImmutableList.of(10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L)));
  }

  @Test
  public void shouldHaveJustEnd() {
    PushOffsetRange offsetRange = new PushOffsetRange(
        Optional.empty(),
        new PushOffsetVector(ImmutableList.of(10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L)));
    assertThat(
        offsetRange.token(),
        is("eyJlbmRPZmZzZXRzIjp7Im9mZnNldHMiOlsxMCw5LDgsNyw2LDUsNCwzXX19")
    );

    PushOffsetRange decoded = PushOffsetRange.fromToken(
        "eyJlbmRPZmZzZXRzIjp7Im9mZnNldHMiOlsxMCw5LDgsNyw2LDUsNCwzXX19");
    assertThat(decoded.getStartOffsets().isPresent(), is(false));
    assertThat(decoded.getEndOffsets().getOffsets(),
        is(ImmutableList.of(10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L)));
  }
}
