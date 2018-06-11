package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class EntityQueryIDTest {
  ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void shouldSerializeCorrectly() throws IOException {
    String id = "query-id";
    String serialized = String.format("\"%s\"", id);

    EntityQueryId deserialized = objectMapper.readValue(serialized, EntityQueryId.class);

    assertThat(deserialized.getId(), equalTo(id));
    assertThat(objectMapper.writeValueAsString(id), equalTo(serialized));
  }
}
