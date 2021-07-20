package io.confluent.ksql.execution.json;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.parser.json.KsqlTypesDeserializationModule;
import org.junit.Test;

public class PlanJsonMapperTest {

  private static final ObjectMapper MAPPER = PlanJsonMapper.INSTANCE.get();

  @Test
  public void shouldEnableFailOnUnknownProperties() {
    assertThat(MAPPER.isEnabled(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES), is(true));
  }

  @Test
  public void shouldEnableFailOnNullProperties() {
    assertThat(MAPPER.isEnabled(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES), is(true));
  }

  @Test
  public void shouldEnableFailOnInvalidSubtype() {
    assertThat(MAPPER.isEnabled(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE), is(true));
  }

  @Test
  public void shouldHaveTypeMapperRegistered() {
    assertThat(
        MAPPER.getRegisteredModuleIds(),
        hasItem(new KsqlTypesDeserializationModule().getTypeId()));
  }
}