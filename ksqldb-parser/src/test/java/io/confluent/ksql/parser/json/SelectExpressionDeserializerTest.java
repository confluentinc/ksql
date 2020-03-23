package io.confluent.ksql.parser.json;

import static io.confluent.ksql.parser.json.SelectExpressionTestCase.SELECT_EXPRESSION;
import static io.confluent.ksql.parser.json.SelectExpressionTestCase.SELECT_EXPRESSION_NAME_NEEDS_QUOTES;
import static io.confluent.ksql.parser.json.SelectExpressionTestCase.SELECT_EXPRESSION_NAME_NEEDS_QUOTES_TXT;
import static io.confluent.ksql.parser.json.SelectExpressionTestCase.SELECT_EXPRESSION_NEEDS_QUOTES;
import static io.confluent.ksql.parser.json.SelectExpressionTestCase.SELECT_EXPRESSION_NEEDS_QUOTES_TXT;
import static io.confluent.ksql.parser.json.SelectExpressionTestCase.SELECT_EXPRESSION_TXT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.execution.plan.SelectExpression;
import java.io.IOException;
import org.junit.Test;

public class SelectExpressionDeserializerTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.registerModule(new KsqlParserSerializationModule());
  }

  @Test
  public void shouldDeserializeSelectExpression() throws IOException {
    assertThat(
        MAPPER.readValue(SELECT_EXPRESSION_TXT, SelectExpression.class),
        equalTo(SELECT_EXPRESSION)
    );
  }

  @Test
  public void shouldDeserializeSelectExpressionNeedingQuotes() throws IOException {
    assertThat(
        MAPPER.readValue(SELECT_EXPRESSION_NEEDS_QUOTES_TXT, SelectExpression.class),
        equalTo(SELECT_EXPRESSION_NEEDS_QUOTES)
    );
  }

  @Test
  public void shouldDeserializeSelectExpressionNeedingQuotesInName() throws IOException {
    assertThat(
        MAPPER.readValue(SELECT_EXPRESSION_NAME_NEEDS_QUOTES_TXT, SelectExpression.class),
        equalTo(SELECT_EXPRESSION_NAME_NEEDS_QUOTES)
    );
  }
}