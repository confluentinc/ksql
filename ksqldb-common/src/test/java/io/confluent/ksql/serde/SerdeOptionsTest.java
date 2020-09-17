package io.confluent.ksql.serde;

import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableSet;
import com.google.common.testing.EqualsTester;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

public class SerdeOptionsTest {

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementHashCodeAndEquals() {
    new EqualsTester()
        .addEqualityGroup(
            SerdeOptions.of(SerdeOption.UNWRAP_SINGLE_VALUES),
            SerdeOptions.of(ImmutableSet.of(SerdeOption.UNWRAP_SINGLE_VALUES))
        )
        .addEqualityGroup(
            SerdeOptions.of()
        )
        .addEqualityGroup(
            SerdeOptions.of(SerdeOption.WRAP_SINGLE_VALUES)
        )
        .testEquals();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnClashingSingleValueWrappingOptions() {
    SerdeOptions.of(SerdeOption.UNWRAP_SINGLE_VALUES, SerdeOption.WRAP_SINGLE_VALUES);
  }

  @Test
  public void shouldImplmentToString() {
    MatcherAssert.assertThat(
        SerdeOptions.of(SerdeOption.UNWRAP_SINGLE_VALUES).toString(),
        is("[UNWRAP_SINGLE_VALUES]"));
  }
}