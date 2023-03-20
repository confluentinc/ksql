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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.parser.SqlBaseParser.VariableNameContext;
import io.confluent.ksql.util.ParserKeywordValidatorUtil;
import io.confluent.ksql.util.ParserUtil;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;

/**
 * This class is a syntax error validator used in the {@link DefaultKsqlParser} class, which
 * validates errors during SQL parsing. It implements {@link BaseErrorListener} so it can be
 * added as an error listener to the {@code SqlBaseLexer}.
 * </p>
 * This class is called for every error caused by ANTlr parsing. Some errors, like reserved keywords
 * in identifiers for some statements, are valid and should be allowed in the syntax, and others
 * errors should return a better error message.
 */
public class SyntaxErrorValidator extends BaseErrorListener {
  private static final Pattern EXTRANEOUS_INPUT_PATTERN = Pattern.compile(
      "extraneous input.*expecting.*");
  private static final Pattern MISMATCHED_INPUT_PATTERN = Pattern.compile(
      "mismatched input.*expecting.*");

  // List of validators per ANTLr context.
  private static final Map<Class<?>, BiFunction<String, RecognitionException, Boolean>>
      ERROR_VALIDATORS = ImmutableMap.of(
      VariableNameContext.class, SyntaxErrorValidator::isValidVariableName
  );

  /**
   * Check if a variable name uses a SQL reserved keyword that should be valid as a variable.
   * </p>
   * Valid reserved keywords in identifiers are not easy to configure in ANTLr {@code SqlBase.g4}.
   * ANTLr rules must specify what a reserved keyword is so they are allowed (see
   * nonReservedKeyword in {@code SqlBase.g4}), but it is not scalable. There are too many keywords
   * used in SqlBase.g4, and not all statements allow the same reserved keywords. This method
   * checks against the whole list of symbolic names (or keywords) from ANTLr, and verifies the
   * variable name matches those.
   *
   * @param errorMessage The error message thrown by ANTLr parsers.
   * @param exception The exception that contains the offending token.
   * @return True if a reserved keyword is a valid variable name; false otherwise.
   */
  private static boolean isValidVariableName(
      final String errorMessage,
      final RecognitionException exception
  ) {
    if (MISMATCHED_INPUT_PATTERN.matcher(errorMessage).matches()) {
      if (exception.getOffendingToken() != null) {
        return ParserKeywordValidatorUtil.getKsqlReservedWords()
            .contains(exception.getOffendingToken().getText());
      }
    }

    return false;
  }

  private static boolean validateErrorContext(
      final RuleContext context,
      final String errorMessage,
      final RecognitionException exception
  ) {
    final BiFunction<String, RecognitionException, Boolean> validator =
        ERROR_VALIDATORS.get(context.getClass());
    if (validator == null) {
      return false;
    }

    return validator.apply(errorMessage, exception);
  }

  private static boolean shouldIgnoreSyntaxError(
      final String errorMessage,
      final RecognitionException exception
  ) {
    if (exception.getCtx() != null) {
      return validateErrorContext(exception.getCtx(), errorMessage, exception);
    }

    return false;
  }

  /**
   * checks if the error is a reserved keyword error by checking the message and offendingSymbol
   * @param message the error message
   * @param offendingSymbol the symbol that caused the error
   * @return True if the error is a reserved keyword
   */
  private static boolean isKeywordError(final String message, final String offendingSymbol) {
    final Matcher m = EXTRANEOUS_INPUT_PATTERN.matcher(message);
    return  m.find() && ParserUtil.isReserved(offendingSymbol);
  }

  @Override
  public void syntaxError(
      final Recognizer<?, ?> recognizer,
      final Object offendingSymbol,
      final int line,
      final int charPositionInLine,
      final String message,
      final RecognitionException e
  ) {
    if (e != null && shouldIgnoreSyntaxError(message, e)) {
      return;
    }

    if (offendingSymbol instanceof Token && isKeywordError(
        message, ((Token) offendingSymbol).getText())) {
      //Checks if the error is a reserved keyword error
      final String tokenName = ((Token) offendingSymbol).getText();
      final String newMessage =
          "\"" + tokenName + "\" is a reserved keyword and it can't be used as an identifier."
              + " You can use it as an identifier by escaping it as \'" + tokenName + "\' ";
      throw new ParsingException(newMessage, line, charPositionInLine);
    } else {
      throw new ParsingException(message, line, charPositionInLine);
    }
  }
}
