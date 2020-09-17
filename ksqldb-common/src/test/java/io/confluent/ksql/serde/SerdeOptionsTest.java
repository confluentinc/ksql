package io.confluent.ksql.serde;

import static io.confluent.ksql.serde.SerdeOption.UNWRAP_SINGLE_KEYS;
import static io.confluent.ksql.serde.SerdeOption.UNWRAP_SINGLE_VALUES;
import static io.confluent.ksql.serde.SerdeOption.WRAP_SINGLE_VALUES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableSet;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.serde.SerdeOptions.Builder;
import java.util.Optional;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

public class SerdeOptionsTest {

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementHashCodeAndEquals() {
    new EqualsTester()
        .addEqualityGroup(
            SerdeOptions.of(UNWRAP_SINGLE_VALUES),
            SerdeOptions.of(ImmutableSet.of(UNWRAP_SINGLE_VALUES))
        )
        .addEqualityGroup(
            SerdeOptions.of()
        )
        .addEqualityGroup(
            SerdeOptions.of(WRAP_SINGLE_VALUES)
        )
        .testEquals();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnClashingSingleValueWrappingOptions() {
    SerdeOptions.of(UNWRAP_SINGLE_VALUES, WRAP_SINGLE_VALUES);
  }

  @Test
  public void shouldBuildOptions() {
    // When:
    final SerdeOptions result = SerdeOptions.builder()
        .add(UNWRAP_SINGLE_KEYS)
        .add(WRAP_SINGLE_VALUES)
        .build();

    // Then:
    assertThat(result, is(SerdeOptions.of(WRAP_SINGLE_VALUES, UNWRAP_SINGLE_KEYS)));
  }

  @Test
  public void shouldThrowOnBuildOnClashingOptions() {
    // Given:
    final Builder builder = SerdeOptions.builder()
        .add(UNWRAP_SINGLE_VALUES);

    // When:
    assertThrows(
        IllegalArgumentException.class,
        () -> builder.add(WRAP_SINGLE_VALUES)
    );

    // Then:
    final SerdeOptions result = builder.build();
    assertThat("should be in valid state", result.all(), not(contains(WRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldReturnKeyWrapping() {
    assertThat(SerdeOptions.of().keyWrapping(), is(Optional.empty()));
    assertThat(SerdeOptions.of(UNWRAP_SINGLE_KEYS).keyWrapping(),
        is(Optional.of(UNWRAP_SINGLE_KEYS)));
  }

  @Test
  public void shouldReturnValueWrapping() {
    assertThat(SerdeOptions.of().valueWrapping(), is(Optional.empty()));
    assertThat(SerdeOptions.of(WRAP_SINGLE_VALUES).valueWrapping(),
        is(Optional.of(WRAP_SINGLE_VALUES)));
    assertThat(SerdeOptions.of(UNWRAP_SINGLE_VALUES).valueWrapping(),
        is(Optional.of(UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldImplmentToString() {
    MatcherAssert.assertThat(
        SerdeOptions.of(SerdeOption.UNWRAP_SINGLE_VALUES).toString(),
        is("[UNWRAP_SINGLE_VALUES]"));
  }
}