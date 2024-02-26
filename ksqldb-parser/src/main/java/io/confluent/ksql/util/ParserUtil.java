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

package io.confluent.ksql.util;

import static io.confluent.ksql.parser.SqlBaseParser.DecimalLiteralContext;
import static java.util.Objects.requireNonNull;

import com.google.common.base.Strings;
import io.confluent.ksql.execution.expression.tree.DecimalLiteral;
import io.confluent.ksql.execution.expression.tree.DoubleLiteral;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.CaseInsensitiveStream;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.ParsingException;
import io.confluent.ksql.parser.SqlBaseLexer;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.SqlBaseParser.FloatLiteralContext;
import io.confluent.ksql.parser.SqlBaseParser.IntegerLiteralContext;
import io.confluent.ksql.parser.SqlBaseParser.NumberContext;
import io.confluent.ksql.parser.SqlBaseParser.SourceNameContext;
import io.confluent.ksql.parser.TokenLocation;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.tree.ColumnConstraints;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang3.text.translate.LookupTranslator;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public final class ParserUtil {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  /**
   * Source names must adhere to the kafka topic naming convention. We restrict
   * it here instead of as a parser rule to allow for a more descriptive error
   * message and to avoid duplicated rules.
   *
   * @see org.apache.kafka.streams.state.StoreBuilder#name
   */
  private static final Pattern VALID_SOURCE_NAMES = Pattern.compile("[a-zA-Z0-9_-]*");

  private static final LookupTranslator ESCAPE_SYMBOLS = new LookupTranslator(
      new String[][]{
          {"'", "''"}
      }
  );

  private ParserUtil() {
  }

  public static SourceName getSourceName(final SourceNameContext sourceName) {
    final String text = getIdentifierText(sourceName.identifier());
    if (!VALID_SOURCE_NAMES.matcher(text).matches()) {
      final String baseMessage = "Illegal argument at "
          + getLocation(sourceName).map(NodeLocation::toString).orElse("?")
          + ". Source names may only contain alphanumeric values, '_' or '-'.";
      throw new ParseFailedException(
          baseMessage,
          baseMessage + " Got: '" + text + "'",
          text
      );
    }
    return SourceName.of(text);
  }

  public static String getIdentifierText(final SqlBaseParser.IdentifierContext context) {
    return getIdentifierText(false, context);
  }

  public static String getIdentifierText(
      final boolean caseSensitive,
      final SqlBaseParser.IdentifierContext context) {
    if (context instanceof SqlBaseParser.QuotedIdentifierAlternativeContext) {
      return unquote(context.getText(), "\"");
    } else if (context instanceof SqlBaseParser.BackQuotedIdentifierContext) {
      return unquote(context.getText(), "`");
    } else {
      return caseSensitive ? context.getText() : context.getText().toUpperCase();
    }
  }

  public static String unquote(final String value, final String quote) {
    return value.substring(1, value.length() - 1)
        .replace(quote + quote, quote);
  }

  public static boolean isQuoted(final String value, final String quote) {
    return value.startsWith(quote) && value.endsWith(quote);
  }

  public static String sanitize(final String value) {
    if (isQuoted(value, "'")) {
      return "'" + escapeString(unquote(value, "'")) + "'";
    }

    return escapeString(value);
  }

  private static String escapeString(final String value) {
    if (value == null || value.isEmpty()) {
      return value;
    }

    return ESCAPE_SYMBOLS.translate(value);
  }

  private static String validateAndUnquote(final String value, final char quote) {
    if (value.charAt(0) != quote) {
      throw new IllegalStateException("Value must begin with quote");
    }
    if (value.charAt(value.length() - 1) != quote || value.length() < 2) {
      throw new IllegalArgumentException("Expected matching quote at end of value");
    }

    int i = 1;
    while (i < value.length() - 1) {
      if (value.charAt(i) == quote) {
        if (value.charAt(i + 1) != quote || i + 1 == value.length() - 1) {
          throw new IllegalArgumentException("Un-escaped quote in middle of value at index " + i);
        }
        i += 2;
      } else {
        i++;
      }
    }

    return value.substring(1, value.length() - 1)
        .replace("" + quote + quote, "" + quote);
  }

  public static int processIntegerNumber(final NumberContext number, final String context) {
    if (number instanceof SqlBaseParser.IntegerLiteralContext) {
      return ((IntegerLiteral) visitIntegerLiteral((IntegerLiteralContext) number)).getValue();
    }
    throw new KsqlException("Value must be integer for command: " + context);
  }

  public static Literal visitIntegerLiteral(final IntegerLiteralContext context) {
    final Optional<NodeLocation> location = getLocation(context);

    final long valueAsLong;
    try {
      valueAsLong = Long.parseLong(context.getText());
    } catch (final NumberFormatException e) {
      throw new ParsingException("Invalid numeric literal: " + context.getText(), location);
    }

    if (valueAsLong <= Integer.MAX_VALUE && valueAsLong >= Integer.MIN_VALUE) {
      return new IntegerLiteral(location, (int) valueAsLong);
    } else {
      return new LongLiteral(location, valueAsLong);
    }
  }

  public static DoubleLiteral parseFloatLiteral(final FloatLiteralContext context) {
    final Optional<NodeLocation> location = getLocation(context);

    try {
      final double value = Double.parseDouble(context.getText());
      if (Double.isNaN(value)) {
        throw new ParsingException("Not a number: " + context.getText(), location);
      }
      if (Double.isInfinite(value)) {
        throw new ParsingException("Number overflows DOUBLE: " + context.getText(), location);
      }
      return new DoubleLiteral(location, value);
    } catch (final NumberFormatException e) {
      throw new ParsingException("Invalid numeric literal: " + context.getText(), location);
    }
  }

  public static DecimalLiteral parseDecimalLiteral(final DecimalLiteralContext context) {
    final Optional<NodeLocation> location = getLocation(context);

    try {
      final String value = context.getText();
      return new DecimalLiteral(location, new BigDecimal(value));
    } catch (final NumberFormatException e) {
      throw new ParsingException("Invalid numeric literal: " + context.getText(), location);
    }
  }

  public static Optional<NodeLocation> getLocation(final TerminalNode terminalNode) {
    requireNonNull(terminalNode, "terminalNode is null");
    return getLocation(terminalNode.getSymbol());
  }

  public static Optional<NodeLocation> getLocation(final ParserRuleContext parserRuleContext) {
    requireNonNull(parserRuleContext, "parserRuleContext is null");
    return getLocation(parserRuleContext.getStart(), parserRuleContext.getStop());
  }

  public static Optional<NodeLocation> getLocation(final Token start, final Token stop) {
    requireNonNull(start, "Start token is null");
    requireNonNull(stop, "Stop token is null");

    final TokenLocation startLocation = new TokenLocation(
        start.getLine(),
        start.getCharPositionInLine(),
        start.getStartIndex(),
        start.getStopIndex()
    );

    final TokenLocation stopLocation = new TokenLocation(
        stop.getLine(),
        stop.getCharPositionInLine(),
        stop.getStartIndex(),
        stop.getStopIndex()
    );

    return Optional.of(new NodeLocation(startLocation, stopLocation));
  }

  public static Optional<NodeLocation> getLocation(final Token token) {
    requireNonNull(token, "token is null");
    return getLocation(token, token);
  }

  /**
   * Checks if the token is a reserved keyword or not
   * @param token the String that caused the parsing error
   * @return true if the token is a reserved keyword according to SqlBase.g4
   *         false otherwise
   */
  public static boolean isReserved(final String token) {

    final SqlBaseLexer sqlBaseLexer = new SqlBaseLexer(
            new CaseInsensitiveStream(CharStreams.fromString(token)));
    final CommonTokenStream tokenStream = new CommonTokenStream(sqlBaseLexer);
    final SqlBaseParser sqlBaseParser = new SqlBaseParser(tokenStream);

    sqlBaseParser.removeErrorListeners();

    final SqlBaseParser.NonReservedContext nonReservedContext = sqlBaseParser.nonReserved();
    if (nonReservedContext.exception == null) {
      // If we call nonReservedWord, and if it successfully parses,
      // then we just parsed through a nonReserved word as defined in SqlBase.g4
      // and we return false
      return false;
    }

    final Set<String> allVocab = ParserKeywordValidatorUtil.getKsqlReservedWords();

    return allVocab.contains(token.toLowerCase());
  }

  public static ColumnConstraints getColumnConstraints(
      final SqlBaseParser.ColumnConstraintsContext context
  ) {
    if (context == null) {
      return ColumnConstraints.NO_COLUMN_CONSTRAINTS;
    }

    ColumnConstraints.Builder builder = new ColumnConstraints.Builder();
    if (context.KEY() != null) {
      if (context.PRIMARY() != null) {
        builder = builder.primaryKey();
      } else {
        builder = builder.key();
      }
    } else if (context.HEADERS() != null) {
      builder = builder.headers();
    } else if (context.HEADER() != null) {
      builder =
          builder.header(Strings.emptyToNull(unquote(context.STRING().getText(), "'").trim()));
    }

    return builder.build();
  }
}
