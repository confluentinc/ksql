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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.apache.commons.text.similarity.LevenshteinDetailedDistance;


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
  private final List<String> commands = Arrays.asList("SELECT", "CREATE", "INSERT", "DESCRIBE",
      "PRINT", "EXPLAIN", "SHOW", "LIST", "TERMINATE", "PAUSE", "RESUME", "DROP", "SET", "DEFINE",
      "UNDEFINE", "UNSET", "ASSERT", "ALTER");


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
   * @return true if the error is a reserved keyword error
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

    if (offendingSymbol instanceof Token) {
      final StringBuilder sb = new StringBuilder();
      sb.append("Syntax Error\n");
      if (isKeywordError(message, ((Token) offendingSymbol).getText())) {
        //Checks if the error is a reserved keyword error
        final String tokenName = ((Token) offendingSymbol).getText();
        sb.append(
            String.format("\"%s\" is a reserved keyword and it can't be used as an identifier."
                + " You can use it as an identifier by escaping it as '%s' ",
                tokenName, tokenName));
      } else if (message.contains("expecting")) {
        sb.append(
            getExpectations(message, ((Token) offendingSymbol).getText())
        );
      } else if (e instanceof NoViableAltException) {
        sb.append(
            String.format(
                "Syntax error at or near '%s' at line %d:%d",
                ((Token) offendingSymbol).getText(), line, charPositionInLine + 1
            )
        );
      } else {
        sb.append(message);
      }
      throw new ParsingException(sb.toString(), line, charPositionInLine);
    } else {
      throw new ParsingException(message, line, charPositionInLine);
    }
  }

  private String getExpectations(
      final String message,
      final String offendingToken) {
    final StringBuilder output = new StringBuilder();
    final String expectingStr = message.split("expecting ")[1];
    // If the command is mistyped, find the most similar command and show it
    if (isCommand(expectingStr)) { // this is a command typo
      output.append(String.format("Unknown statement '%s'%n", offendingToken));
      output.append(
              String.format("Did you mean '%s'?", getMostSimilar(offendingToken)));
    // In case of missing closing brackets or parentheses
    } else if (message.contains("EOF")) {
      output.append("Syntax error at or near \";\"\n");
      output.append("'}', ']', or ')' is missing");
    // If the expecting list generated by Antlr is a short one, show it to user
    } else if (expectingStr.split(",").length <= 3) {
      output.append(String.format("Expecting %s", expectingStr));
    }
    return output.toString();
  }

  private boolean isCommand(final String expectingStr) {
    final List<String> expectedList = Arrays.asList(
        expectingStr.replace("{","").replace("}","")
            .replace("<EOF>,","").replace(" '","")
            .replace("'","").split(",")
    );
    return expectedList.equals(commands);
  }

  private String getMostSimilar(final String target) {
    final LevenshteinDetailedDistance computer = LevenshteinDetailedDistance.getDefaultInstance();
    int min = Integer.MAX_VALUE;
    String output = "";
    for (String command:commands) {
      final int distance = computer.apply(command, target.toUpperCase()).getDistance();
      if (distance < min) {
        min = distance;
        output = command;
      }
    }
    return output;
  }
}
