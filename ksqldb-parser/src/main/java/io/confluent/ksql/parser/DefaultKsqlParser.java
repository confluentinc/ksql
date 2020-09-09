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

package io.confluent.ksql.parser;

import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.parser.SqlBaseParser.SingleStatementContext;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.tree.Statement;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.misc.ParseCancellationException;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class DefaultKsqlParser implements KsqlParser {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final BaseErrorListener ERROR_LISTENER = new BaseErrorListener() {
    @Override
    public void syntaxError(
        final Recognizer<?, ?> recognizer,
        final Object offendingSymbol,
        final int line,
        final int charPositionInLine,
        final String message,
        final RecognitionException e
    ) {
      if (isKeywordError(message, offendingSymbol)) {
        final String tokenName = ((CommonToken) offendingSymbol).getText();
        final String newMessage =
                "\"" + tokenName + "\" is a reserved keyword and it can't be used as an identifier";
        throw new ParsingException(newMessage, e, line, charPositionInLine);
      } else {
        throw new ParsingException(message, e, line, charPositionInLine);
      }
    }
  };

  @Override
  public List<ParsedStatement> parse(final String sql) {
    try {
      final SqlBaseParser.StatementsContext statementsContext = getParseTree(sql);

      return statementsContext.singleStatement().stream()
          .map(DefaultKsqlParser::parsedStatement)
          .collect(Collectors.toList());

    } catch (final Exception e) {
      throw new ParseFailedException(e.getMessage(), sql, e);
    }
  }

  public static ParsedStatement parsedStatement(final SingleStatementContext statement) {
    return ParsedStatement.of(
        getStatementString(statement),
        statement
    );
  }

  @Override
  public PreparedStatement<?> prepare(
      final ParsedStatement stmt,
      final TypeRegistry typeRegistry
  ) {
    try {
      final AstBuilder astBuilder = new AstBuilder(typeRegistry);
      final Statement root = astBuilder.buildStatement(stmt.getStatement());

      return PreparedStatement.of(stmt.getStatementText(), root);
    } catch (final ParseFailedException e) {
      if (!e.getSqlStatement().isEmpty()) {
        throw e;
      }
      throw new ParseFailedException(
          e.getRawMessage(), stmt.getStatementText(), e.getCause());
    } catch (final Exception e) {
      throw new ParseFailedException(
          "Failed to prepare statement: " + e.getMessage(), stmt.getStatementText(), e);
    }
  }

  private static SqlBaseParser.StatementsContext getParseTree(final String sql) {

    final SqlBaseLexer sqlBaseLexer = new SqlBaseLexer(
        new CaseInsensitiveStream(CharStreams.fromString(sql)));
    final CommonTokenStream tokenStream = new CommonTokenStream(sqlBaseLexer);
    final SqlBaseParser sqlBaseParser = new SqlBaseParser(tokenStream);

    sqlBaseLexer.removeErrorListeners();
    sqlBaseLexer.addErrorListener(ERROR_LISTENER);

    sqlBaseParser.removeErrorListeners();
    sqlBaseParser.addErrorListener(ERROR_LISTENER);

    final Function<SqlBaseParser, ParserRuleContext> parseFunction = SqlBaseParser::statements;

    try {
      // first, try parsing with potentially faster SLL mode
      sqlBaseParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
      return (SqlBaseParser.StatementsContext)parseFunction.apply(sqlBaseParser);
    } catch (final ParseCancellationException ex) {
      // if we fail, parse with LL mode
      tokenStream.seek(0); // rewind input stream
      sqlBaseParser.reset();

      sqlBaseParser.getInterpreter().setPredictionMode(PredictionMode.LL);
      return (SqlBaseParser.StatementsContext)parseFunction.apply(sqlBaseParser);
    }
  }

  private static String getStatementString(final SingleStatementContext singleStatementContext) {
    final CharStream charStream = singleStatementContext.start.getInputStream();
    return charStream.getText(Interval.of(
        singleStatementContext.start.getStartIndex(),
        singleStatementContext.stop.getStopIndex()
    ));
  }

  private static boolean isKeywordError(final String message, final Object offendingSymbol) {
    final String tokenName = ((CommonToken) offendingSymbol).getText().toLowerCase();

    final Pattern keywordPattern = Pattern.compile("extraneous input.*expecting.*");
    final Matcher m = keywordPattern.matcher(message);
    return reservedKeywords.contains(tokenName) && m.find();
  }

  private static final HashSet<String> reservedKeywords = new HashSet<>(Arrays.asList("advance",
          "all",
          "and",
          "as",
          "at",
          "beginning",
          "between",
          "by",
          "case",
          "cast",
          "catalog",
          "connector",
          "connectors",
          "create",
          "days",
          "decimal",
          "delete",
          "describe",
          "distinct",
          "drop",
          "else",
          "end",
          "exists",
          "export",
          "extended",
          "false",
          "from",
          "full",
          "grace",
          "group",
          "having",
          "hopping",
          "hours",
          "in",
          "inner",
          "insert",
          "into",
          "is",
          "join",
          "left",
          "like",
          "limit",
          "list",
          "load",
          "materialized",
          "millisecond",
          "milliseconds",
          "minutes",
          "months",
          "namespace",
          "not",
          "null",
          "on",
          "or",
          "outer",
          "period",
          "print",
          "properties",
          "queries",
          "query",
          "rename",
          "retention",
          "right",
          "run",
          "sample",
          "script",
          "seconds",
          "select",
          "size",
          "stream",
          "streams",
          "table",
          "terminate",
          "then",
          "to",
          "topic",
          "topics",
          "true",
          "tumbling",
          "unset",
          "values",
          "view",
          "when",
          "where",
          "window",
          "with",
          "within",
          "years"));
}
