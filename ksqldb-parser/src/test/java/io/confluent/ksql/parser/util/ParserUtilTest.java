/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.parser.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.ksql.parser.ParsingException;
import io.confluent.ksql.parser.SqlBaseParser.DecimalLiteralContext;
import io.confluent.ksql.parser.SqlBaseParser.FloatLiteralContext;
import io.confluent.ksql.util.ParserUtil;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ParserUtilTest {

  @Mock
  private DecimalLiteralContext decimalLiteralContext;

  @Mock
  private FloatLiteralContext floatLiteralContext;

  @Before
  public void setUp() {
    mockLocation(decimalLiteralContext, 1, 2);
    mockLocation(floatLiteralContext, 1, 2);
  }

  @Test
  public void shouldThrowWhenParsingDecimalIfNaN() {
    // Given:
    when(decimalLiteralContext.getText()).thenReturn("NaN");

    // When:
    final ParsingException e = assertThrows(
        ParsingException.class,
        () -> ParserUtil.parseDecimalLiteral(decimalLiteralContext)
    );

    // Then:
    assertThat(e.getUnloggedDetails(), containsString("line 1:4: Invalid numeric literal: NaN"));
    assertThat(e.getMessage(), containsString("line 1:4: Syntax error at line 1:4"));
  }

  @Test
  public void shouldThrowWhenParsingFloatIfNaN() {
    // Given:
    when(floatLiteralContext.getText()).thenReturn("NaN");

    // When:
    final ParsingException e = assertThrows(
        ParsingException.class,
        () -> ParserUtil.parseFloatLiteral(floatLiteralContext)
    );

    // Then:
    assertThat(e.getUnloggedDetails(), containsString("line 1:4: Not a number: NaN"));
    assertThat(e.getMessage(), containsString("line 1:4: Syntax error at line 1:4"));
  }

  @Test
  public void shouldThrowWhenParsingDecimalIfNotDecimal() {
    // Given:
    when(decimalLiteralContext.getText()).thenReturn("What?");

    // When:
    final ParsingException e = assertThrows(
        ParsingException.class,
        () -> ParserUtil.parseDecimalLiteral(decimalLiteralContext)
    );

    // Then:
    assertThat(e.getUnloggedDetails(), containsString("line 1:4: Invalid numeric literal: What?"));
    assertThat(e.getMessage(), containsString("line 1:4: Syntax error at line 1:4"));
  }

  @Test
  public void shouldThrowWhenParsingDecimalIfOverflowsDouble() {
    // Given:
    when(floatLiteralContext.getText()).thenReturn("1.7976931348623159E308");

    // When:
    final ParsingException e = assertThrows(
        ParsingException.class,
        () -> ParserUtil.parseFloatLiteral(floatLiteralContext)
    );

    // Then:
    assertThat(e.getUnloggedDetails(), containsString("line 1:4: Number overflows DOUBLE: 1.7976931348623159E308"));
    assertThat(e.getMessage(), containsString("line 1:4: Syntax error at line 1:4"));
  }

  private static void mockLocation(final ParserRuleContext ctx, final int line, final int col) {
    final Token startToken = mock(Token.class);
    when(startToken.getLine()).thenReturn(line);
    when(startToken.getCharPositionInLine()).thenReturn(col);
    when(ctx.getStart()).thenReturn(startToken);

    final Token stopToken = mock(Token.class);
    when(ctx.getStop()).thenReturn(stopToken);
  }
}
