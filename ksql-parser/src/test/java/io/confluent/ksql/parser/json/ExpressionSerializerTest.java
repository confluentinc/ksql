package io.confluent.ksql.parser.json;

import static io.confluent.ksql.parser.json.ExpressionTestCase.EXPRESSION;
import static io.confluent.ksql.parser.json.ExpressionTestCase.EXPRESSION_NEEDS_QUOTES;
import static io.confluent.ksql.parser.json.ExpressionTestCase.EXPRESSION_NEEDS_QUOTES_TXT;
import static io.confluent.ksql.parser.json.ExpressionTestCase.EXPRESSION_TXT;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.junit.Test;

public class ExpressionSerializerTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.registerModule(new KsqlParserSerializationModule());
  }

  @Test
  public void shouldSerializeExpression() throws IOException {
    assertThat(MAPPER.writeValueAsString(EXPRESSION), equalTo(EXPRESSION_TXT));
  }

  @Test
  public void shouldSerializeExpressionNeedingQuotes() throws IOException {
    assertThat(
        MAPPER.writeValueAsString(EXPRESSION_NEEDS_QUOTES),
        equalTo(EXPRESSION_NEEDS_QUOTES_TXT)
    );
  }
}