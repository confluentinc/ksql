package io.confluent.ksql.test.tools;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;

import java.math.BigDecimal;
import java.util.Map;
import org.junit.Test;

public class TestJsonMapperTest {

  @Test
  public void shouldLoadExactDecimals() throws Exception {
    // Given:
    final String json = "{\"DEC\": 1.0000}";

    // When:
    final Map<?, ?> result = TestJsonMapper.INSTANCE.get().readValue(json, Map.class);

    // Then:
    assertThat(result, hasEntry("DEC", new BigDecimal("1.0000")));
  }
}