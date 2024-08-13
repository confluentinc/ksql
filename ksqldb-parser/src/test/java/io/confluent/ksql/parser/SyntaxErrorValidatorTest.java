/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.ksql.parser.SqlBaseParser.CreateStreamContext;
import io.confluent.ksql.parser.SqlBaseParser.VariableNameContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.easymock.EasyMockRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class SyntaxErrorValidatorTest {
  private SyntaxErrorValidator syntaxErrorValidator = new SyntaxErrorValidator();

  @Test
  public void shouldAllowReservedKeywordsOnVariableName() {
    // Given:
    final String errorMessage = "mismatched input 'topic' expecting IDENTIFIER";
    final RecognitionException exception =
        givenException(mock(VariableNameContext.class), getToken("topic"));

    // When/Then:
    callSyntaxError(errorMessage, exception);
  }

  @Test
  public void shouldThrowDefaultParsingExceptionIfExceptionIsNull() {
    // Given:
    final String errorMessage = "mismatched input 'topic' expecting IDENTIFIER";
    final RecognitionException exception = null;

    // When
    final ParsingException e = assertThrows(
        ParsingException.class,
        () -> callSyntaxError(errorMessage, exception)
    );

    // Then:
    assertThat(e.getUnloggedDetails(),
        containsString("mismatched input 'topic' expecting IDENTIFIER"));
    assertThat(e.getMessage(),
        containsString("line 0:1: Syntax error at line 0:1"));
  }

  @Test
  public void shouldThrowDefaultParsingExceptionIfContextIsNull() {
    // Given:
    final String errorMessage = "mismatched input 'topic' expecting IDENTIFIER";
    final RecognitionException exception = givenException(null, getToken("topic"));

    // When
    final ParsingException e = assertThrows(
        ParsingException.class,
        () -> callSyntaxError(errorMessage, exception)
    );

    // Then:
    assertThat(e.getUnloggedDetails(), containsString("Syntax Error\nExpecting IDENTIFIER"));
    assertThat(e.getMessage(), containsString("line 0:1: Syntax error at line 0:1"));
  }

  @Test
  public void shouldThrowOnInvalidNonReservedKeywordVariableName() {
    // Given:
    final String errorMessage = "mismatched input '1' expecting IDENTIFIER";
    final RecognitionException exception =
        givenException(mock(VariableNameContext.class), getToken("1"));

    // When
    final ParsingException e = assertThrows(
        ParsingException.class,
        () -> callSyntaxError(errorMessage, exception)
    );

    // Then:
    assertThat(e.getUnloggedDetails(),
        containsString("line 0:1: Syntax Error\nExpecting IDENTIFIER"));
    assertThat(e.getMessage(), containsString("line 0:1: Syntax error at line 0:1"));
  }

  @Test
  public void shouldThrowWhenReservedKeywordUsedAsIdentifierOnNoVariablesNames() {
    // Given:
    final String errorMessage = "extraneous input 'size' expecting IDENTIFIER";
    final RecognitionException exception =
        givenException(mock(CreateStreamContext.class), getToken("size"));

    // When:
    final ParsingException e = assertThrows(
        ParsingException.class,
        () -> callSyntaxError(errorMessage, exception)
    );

    // Then:
    assertThat(e.getUnloggedDetails(),
        containsString("\"size\" is a reserved keyword and it can't be used as an identifier"));
    assertThat(e.getMessage(),
        containsString("line 0:1: Syntax error at line 0:1"));
  }

  private void callSyntaxError(final String errorMessage, final RecognitionException exception) {
    syntaxErrorValidator.syntaxError(
        null,
        (exception != null) ? exception.getOffendingToken() : null,
        0,
        0,
        errorMessage,
        exception
    );
  }

  private RecognitionException givenException(
      final RuleContext context,
      final Token offendingToken
  ) {
    final RecognitionException exception =
        new RecognitionException("message", null, null, (ParserRuleContext) context) {
          @Override
          public Token getOffendingToken() {
            return offendingToken;
          }
        };
    return exception;
  }

  private Token getToken(final String text) {
    final Token token = mock(Token.class);
    when(token.getText()).thenReturn(text);
    return token;
  }
}
