package io.confluent.ksql.rest.entity;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.rest.entity.SourceInfo.Stream;
import io.confluent.ksql.rest.entity.SourceInfo.Table;
import org.junit.Test;

public class SourceInfoTest {

  private static final ObjectMapper MAPPER = ApiJsonMapper.INSTANCE.get();

  @Test
  public void shouldDeserializeLegacyStream() throws Exception {
    // Given:
    final String legacyJson = "{"
        + "\"type\":\"STREAM\","
        + "\"name\":\"vic\","
        + "\"topic\":\"bob\","
        + "\"format\":\"john\""
        + "}";

    // When:
    final SourceInfo result = MAPPER.readValue(legacyJson, SourceInfo.class);

    // Then:
    assertThat(result, is(new Stream("vic", "bob", "KAFKA", "john", false)));
  }

  @Test
  public void shouldDeserializeLegacyTable() throws Exception {
    // Given:
    final String legacyJson = "{"
        + "\"type\":\"TABLE\","
        + "\"name\":\"vic\","
        + "\"topic\":\"bob\","
        + "\"format\":\"john\""
        + "}";

    // When:
    final SourceInfo result = MAPPER.readValue(legacyJson, SourceInfo.class);

    // Then:
    assertThat(result, is(new Table("vic", "bob", "KAFKA", "john", false)));
  }
}