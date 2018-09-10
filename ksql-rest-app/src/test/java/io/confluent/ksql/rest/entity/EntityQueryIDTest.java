package io.confluent.ksql.rest.entity;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.rest.util.JsonMapper;
import java.io.IOException;
import org.junit.Test;

public class EntityQueryIDTest {
  final ObjectMapper objectMapper = JsonMapper.INSTANCE.mapper;

  @Test
  public void shouldSerializeCorrectly() throws IOException {
    final String id = "query-id";
    final String serialized = String.format("\"%s\"", id);

    final EntityQueryId deserialized = objectMapper.readValue(serialized, EntityQueryId.class);

    assertThat(deserialized.getId(), equalTo(id));
    assertThat(objectMapper.writeValueAsString(id), equalTo(serialized));
  }
}
