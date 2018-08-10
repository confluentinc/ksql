package io.confluent.ksql.parser.util;

import io.confluent.ksql.util.ParserUtil;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class ParserUtilTest {
  @Test
  public void shouldEscapeStringIfLiteral() {
    assertThat(ParserUtil.escapeIfLiteral("END"), equalTo("`END`"));
  }

  @Test
  public void shouldNotEscapeStringIfNotLiteral() {
    assertThat(ParserUtil.escapeIfLiteral("NOT_A_LITERAL"), equalTo("NOT_A_LITERAL"));
  }
}
