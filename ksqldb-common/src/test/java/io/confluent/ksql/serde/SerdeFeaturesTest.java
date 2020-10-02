package io.confluent.ksql.serde;

import static io.confluent.ksql.serde.SerdeFeature.UNWRAP_SINGLES;
import static io.confluent.ksql.serde.SerdeFeature.WRAP_SINGLES;
import static io.confluent.ksql.serde.SerdeFeatures.WRAPPING_FEATURES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableSet;
import com.google.common.testing.EqualsTester;
import java.util.EnumSet;
import java.util.Optional;
import org.junit.Test;

public class SerdeFeaturesTest {

  private static final ObjectMapper MAPPER = new ObjectMapper()
      .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
      .registerModule(new Jdk8Module());

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldImplementHashCodeAndEquals() {
    new EqualsTester()
        .addEqualityGroup(
            SerdeFeatures.of(UNWRAP_SINGLES),
            SerdeFeatures.from(ImmutableSet.of(UNWRAP_SINGLES))
        )
        .addEqualityGroup(
            SerdeFeatures.of(WRAP_SINGLES)
        )
        .testEquals();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnIncompatibleFeatures() {
    // When:
    SerdeFeatures.of(WRAP_SINGLES, UNWRAP_SINGLES);
  }

  @Test
  public void shouldHaveSensibleToString() {
    // Given:
    final SerdeFeatures features = SerdeFeatures.of(WRAP_SINGLES);

    // Then:
    assertThat(features.toString(), is("[WRAP_SINGLES]"));
  }

  @Test
  public void shouldReturnEmptyFromFindAnyOnNoMatch() {
    assertThat(SerdeFeatures.of()
        .findAny(WRAPPING_FEATURES), is(Optional.empty()));
  }

  @Test
  public void shouldReturnFeatureFromFindAnyOnAMatch() {
    assertThat(SerdeFeatures.of(WRAP_SINGLES)
        .findAny(WRAPPING_FEATURES), is(Optional.of(WRAP_SINGLES)));

    assertThat(SerdeFeatures.of(UNWRAP_SINGLES)
        .findAny(WRAPPING_FEATURES), is(Optional.of(UNWRAP_SINGLES)));
  }

  @Test
  public void shouldHandleIncompatibleInFindAnyParam() {
    assertThat(SerdeFeatures.of(WRAP_SINGLES)
        .findAny(EnumSet.allOf(SerdeFeature.class)), is(not(Optional.empty())));
  }

  @Test
  public void shouldSerializeAsValue() throws Exception {
    // Given:
    final SerdeFeatures features = SerdeFeatures.of(WRAP_SINGLES);

    // When:
    final String json = MAPPER.writeValueAsString(features);

    // Then:
    assertThat(json, is("[\"WRAP_SINGLES\"]"));
  }

  @Test
  public void shouldDeserializeFromValue() throws Exception {
    // Given:
    final String json = "[\"UNWRAP_SINGLES\"]";

    // When:
    final SerdeFeatures result = MAPPER.readValue(json, SerdeFeatures.class);

    // Then:
    assertThat(result, is(SerdeFeatures.of(UNWRAP_SINGLES)));
  }

  @Test
  public void shouldIncludeFeaturesIfNotEmpty() throws Exception {
    // Given:
    final Pojo formats = new Pojo(SerdeFeatures.of(UNWRAP_SINGLES));

    // When:
    final String json = MAPPER.writeValueAsString(formats);

    // Then:
    assertThat(json, containsString("\"features\":[\"UNWRAP_SINGLES\"]"));
  }

  @Test
  public void shouldExcludeFeaturesIfEmpty() throws Exception {
    // Given:
    final Pojo formats = new Pojo(SerdeFeatures.of());

    // When:
    final String json = MAPPER.writeValueAsString(formats);

    // Then:
    assertThat(json, is("{}"));
  }

  static final class Pojo {

    private final SerdeFeatures features;

    Pojo(final SerdeFeatures features) {
      this.features = features;
    }

    @JsonInclude(value = Include.CUSTOM, valueFilter = SerdeFeatures.NOT_EMPTY.class)
    public SerdeFeatures getFeatures() {
      return features;
    }
  }
}