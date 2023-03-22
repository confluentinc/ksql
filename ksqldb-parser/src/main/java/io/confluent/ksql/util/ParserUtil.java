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

import io.confluent.ksql.execution.expression.tree.DecimalLiteral;
import io.confluent.ksql.execution.expression.tree.DoubleLiteral;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.ParsingException;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.SqlBaseParser.FloatLiteralContext;
import io.confluent.ksql.parser.SqlBaseParser.IntegerLiteralContext;
import io.confluent.ksql.parser.SqlBaseParser.NumberContext;
import io.confluent.ksql.parser.SqlBaseParser.SourceNameContext;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.vertx.core.json.JsonObject;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

public final class ParserUtil {

  /**
   * Source names must adhere to the kafka topic naming convention. We restrict
   * it here instead of as a parser rule to allow for a more descriptive error
   * message and to avoid duplicated rules.
   *
   * @see org.apache.kafka.streams.state.StoreBuilder#name
   */
  private static final Pattern VALID_SOURCE_NAMES = Pattern.compile("[a-zA-Z0-9_-]*");

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

  public static String getIdentifierText(final String text) {
    if (text.isEmpty()) {
      return "";
    }

    final char firstChar = text.charAt(0);
    if (firstChar == '`' || firstChar == '"') {
      return validateAndUnquote(text, firstChar);
    }

    return text.toUpperCase();
  }

  public static String unquote(final String value, final String quote) {
    return value.substring(1, value.length() - 1)
        .replace(quote + quote, quote);
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
    return getLocation(parserRuleContext.getStart());
  }

  public static Optional<NodeLocation> getLocation(final Token token) {
    requireNonNull(token, "token is null");
    return Optional.of(new NodeLocation(token.getLine(), token.getCharPositionInLine()));
  }

  public static JsonObject convertJsonFieldCase(final JsonObject obj) {
    final Map<String, Object> convertedMap = new HashMap<>();
    for (Map.Entry<String, Object> entry : obj.getMap().entrySet()) {
      final String key;
      try {
        key = ParserUtil.getIdentifierText(entry.getKey());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(String.format(
            "Invalid column / struct field name. Name: %s. Reason: %s.",
            entry.getKey(),
            e.getMessage()
        ));
      }
      if (convertedMap.containsKey(key)) {
        throw new IllegalArgumentException("Found duplicate column / struct field name: " + key);
      }
      convertedMap.put(key, entry.getValue());
    }
    return new JsonObject(convertedMap);
  }
}
