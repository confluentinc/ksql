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

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.parser.ParsingException;
import io.confluent.ksql.parser.SqlBaseLexer;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.SqlBaseParser.IntegerLiteralContext;
import io.confluent.ksql.parser.SqlBaseParser.NumberContext;
import io.confluent.ksql.parser.tree.IntegerLiteral;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.parser.tree.LongLiteral;
import io.confluent.ksql.parser.tree.NodeLocation;
import io.confluent.ksql.parser.tree.QualifiedName;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang3.ObjectUtils;

public final class ParserUtil {

  public static final Set<String> RESERVED_WORDS;

  static {
    final ImmutableSet.Builder<String> reservedWords = new ImmutableSet.Builder<>();
    for (int i = 1; i < SqlBaseParser.VOCABULARY.getMaxTokenType(); i++) {
      final String name = ObjectUtils.firstNonNull(
          SqlBaseParser.VOCABULARY.getLiteralName(i),
          SqlBaseParser.VOCABULARY.getSymbolicName(i)
      );
      if (name == null) {
        throw new IllegalStateException("Did not expect to unknown word in vocab at index " + i);
      }
      reservedWords.add(StringUtil.cleanQuotes(name));
    }
    RESERVED_WORDS = reservedWords.build();
  }

  private ParserUtil() {
  }

  private static final Set<String> LITERALS_SET = ImmutableSet.copyOf(
      IntStream.range(0, SqlBaseLexer.VOCABULARY.getMaxTokenType())
          .mapToObj(SqlBaseLexer.VOCABULARY::getLiteralName)
          .filter(Objects::nonNull)
          // literals start and end with ' - remove them
          .map(l -> l.substring(1, l.length() - 1))
          .map(String::toUpperCase)
          .collect(Collectors.toSet())
  );

  public static String escapeIfLiteral(final String name) {
    return LITERALS_SET.contains(name.toUpperCase()) ? "`" + name + "`" : name;
  }

  public static String getIdentifierText(final SqlBaseParser.IdentifierContext context) {
    if (context instanceof SqlBaseParser.QuotedIdentifierAlternativeContext) {
      return unquote(context.getText(), "\"");
    } else if (context instanceof SqlBaseParser.BackQuotedIdentifierContext) {
      return unquote(context.getText(), "`");
    } else {
      return context.getText().toUpperCase();
    }
  }

  public static String unquote(final String value, final String quote) {
    return value.substring(1, value.length() - 1)
        .replace(quote + quote, quote);
  }

  public static QualifiedName getQualifiedName(final SqlBaseParser.QualifiedNameContext context) {
    final List<String> parts = context
        .identifier().stream()
        .map(ParserUtil::getIdentifierText)
        .collect(toList());

    return QualifiedName.of(parts);
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
    if (valueAsLong < 0) {
      throw new RuntimeException("Unexpected negative value in literal: " + valueAsLong);
    }

    if (valueAsLong <= Integer.MAX_VALUE) {
      return new IntegerLiteral(location, (int) valueAsLong);
    } else {
      return new LongLiteral(location, valueAsLong);
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
}
