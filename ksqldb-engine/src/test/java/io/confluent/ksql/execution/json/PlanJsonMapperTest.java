package io.confluent.ksql.execution.json;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

public class PlanJsonMapperTest {
  private static final ObjectMapper MAPPER = PlanJsonMapper.create();

  @Test
  public void shouldEnableFailOnUnknownProperties() {
    assertThat(MAPPER.isEnabled(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES), is(true));
  }

  @Test
  public void shouldEnableFailOnNullPrimitives() {
    assertThat(MAPPER.isEnabled(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES), is(true));
  }

  @Test
  public void shouldEnableFailOnNullProperties() {
    assertThat(MAPPER.isEnabled(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES), is(true));
  }

  @Test
  public void shouldEnableFailOnInvalidSubtype() {
    assertThat(MAPPER.isEnabled(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE), is(true));
  }
}