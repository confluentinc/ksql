package io.confluent.ksql.serde;

import static io.confluent.ksql.serde.SerdeFeature.UNWRAP_SINGLES;
import static io.confluent.ksql.serde.SerdeFeature.WRAP_SINGLES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableSet;
import com.google.common.testing.EqualsTester;
import org.junit.Test;

public class EnabledSerdeFeaturesTest {

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementHashCodeAndEquals() {
    new EqualsTester()
        .addEqualityGroup(
            EnabledSerdeFeatures.of(UNWRAP_SINGLES),
            EnabledSerdeFeatures.from(ImmutableSet.of(UNWRAP_SINGLES))
        )
        .addEqualityGroup(
            EnabledSerdeFeatures.of(WRAP_SINGLES)
        )
        .testEquals();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnIncompatibleFeatures() {
    EnabledSerdeFeatures.of(WRAP_SINGLES, UNWRAP_SINGLES);
  }

  @Test
  public void shouldHaveSensibleToString() {
    // Given:
    final EnabledSerdeFeatures features = EnabledSerdeFeatures.of(SerdeFeature.WRAP_SINGLES);

    // Then:
    assertThat(features.toString(), is("[WRAP_SINGLES]"));
  }
}