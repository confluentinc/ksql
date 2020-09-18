package io.confluent.ksql.serde;

import static io.confluent.ksql.serde.SerdeOption.UNWRAP_SINGLE_KEYS;
import static io.confluent.ksql.serde.SerdeOption.UNWRAP_SINGLE_VALUES;
import static io.confluent.ksql.serde.SerdeOption.WRAP_SINGLE_VALUES;
import static io.confluent.ksql.serde.SerdeOptions.KEY_WRAPPING_OPTIONS;
import static io.confluent.ksql.serde.SerdeOptions.VALUE_WRAPPING_OPTIONS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableSet;
import com.google.common.testing.EqualsTester;
import io.confluent.ksql.serde.SerdeOptions.Builder;
import java.util.EnumSet;
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
    assertThat("should be in invalid state", result.all(), not(contains(WRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldReturnKeyFeatures() {
    assertThat(SerdeOptions.of().keyFeatures(), is(EnabledSerdeFeatures.of()));
    assertThat(SerdeOptions.of(WRAP_SINGLE_VALUES, UNWRAP_SINGLE_KEYS).keyFeatures(),
        is(EnabledSerdeFeatures.of(SerdeFeature.UNWRAP_SINGLES)));
  }

  @Test
  public void shouldReturnValueFeatures() {
    assertThat(SerdeOptions.of().valueFeatures(), is(EnabledSerdeFeatures.of()));
    assertThat(SerdeOptions.of(WRAP_SINGLE_VALUES, UNWRAP_SINGLE_KEYS).valueFeatures(),
        is(EnabledSerdeFeatures.of(SerdeFeature.WRAP_SINGLES)));
  }

  @Test
  public void shouldReturnEmptyFromFindAnyOnNoMatch() {
    assertThat(SerdeOptions.of()
        .findAny(VALUE_WRAPPING_OPTIONS), is(Optional.empty()));

    assertThat(SerdeOptions.of(UNWRAP_SINGLE_KEYS)
        .findAny(VALUE_WRAPPING_OPTIONS), is(Optional.empty()));
  }

  @Test
  public void shouldReturnAnOptionFromFindAnyOnAMatch() {
    assertThat(SerdeOptions.of(WRAP_SINGLE_VALUES, UNWRAP_SINGLE_KEYS)
        .findAny(VALUE_WRAPPING_OPTIONS), is(Optional.of(WRAP_SINGLE_VALUES)));

    assertThat(SerdeOptions.of(UNWRAP_SINGLE_VALUES, UNWRAP_SINGLE_KEYS)
        .findAny(KEY_WRAPPING_OPTIONS), is(Optional.of(UNWRAP_SINGLE_KEYS)));
  }

  @Test
  public void shouldHandleIncompatibleOptionsInFindAnyParam() {
    assertThat(SerdeOptions.of(WRAP_SINGLE_VALUES, UNWRAP_SINGLE_KEYS)
        .findAny(EnumSet.allOf(SerdeOption.class)), is(not(Optional.empty())));
  }

  @Test
  public void shouldImplementToString() {
    MatcherAssert.assertThat(
        SerdeOptions.of(SerdeOption.UNWRAP_SINGLE_VALUES).toString(),
        is("[UNWRAP_SINGLE_VALUES]"));
  }
}