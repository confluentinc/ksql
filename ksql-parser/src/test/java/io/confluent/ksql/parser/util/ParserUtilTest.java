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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.ksql.parser.ParsingException;
import io.confluent.ksql.parser.SqlBaseParser.DecimalLiteralContext;
import io.confluent.ksql.util.ParserUtil;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ParserUtilTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private DecimalLiteralContext decimalLiteralContext;

  @Before
  public void setUp() {
    mockLocation(decimalLiteralContext, 1, 2);
  }

  @Test
  public void shouldEscapeStringIfLiteral() {
    assertThat(ParserUtil.escapeIfReservedIdentifier("END"), equalTo("`END`"));
  }

  @Test
  public void shouldNotEscapeStringIfNotLiteral() {
    assertThat(ParserUtil.escapeIfReservedIdentifier("NOT_A_LITERAL"), equalTo("NOT_A_LITERAL"));
  }

  @Test
  public void shouldThrowWhenParsingDecimalIfNaN() {
    // Given:
    when(decimalLiteralContext.getText()).thenReturn("NaN");

    // Then:
    expectedException.expect(ParsingException.class);
    expectedException.expectMessage("line 1:4: Not a number: NaN");

    // When:
    ParserUtil.parseDecimalLiteral(decimalLiteralContext);
  }

  @Test
  public void shouldThrowWhenParsingDecimalIfNotDecimal() {
    // Given:
    when(decimalLiteralContext.getText()).thenReturn("What?");

    // Then:
    expectedException.expect(ParsingException.class);
    expectedException.expectMessage("line 1:4: Invalid numeric literal: What?");

    // When:
    ParserUtil.parseDecimalLiteral(decimalLiteralContext);
  }

  @Test
  public void shouldThrowWhenParsingDecimalIfOverflowsDouble() {
    // Given:
    when(decimalLiteralContext.getText()).thenReturn("1.7976931348623159E308");

    // Then:
    expectedException.expect(ParsingException.class);
    expectedException.expectMessage("line 1:4: Number overflows DOUBLE: 1.7976931348623159E308");

    // When:
    ParserUtil.parseDecimalLiteral(decimalLiteralContext);
  }

  @Test
  public void shouldHaveReservedLiteralInReservedSet() {
    assertThat(ParserUtil.isReservedIdentifier("FROM"), is(true));
  }

  @Test
  public void shouldExcludeNonReservedLiteralsFromReservedSet() {
    // i.e. those in the "nonReserved" rule in SqlBase.g4
    assertThat(ParserUtil.isReservedIdentifier("SHOW"), is(false));
  }

  private static void mockLocation(final ParserRuleContext ctx, final int line, final int col) {
    final Token token = mock(Token.class);
    when(token.getLine()).thenReturn(line);
    when(token.getCharPositionInLine()).thenReturn(col);
    when(ctx.getStart()).thenReturn(token);
  }
}
