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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.metastore.TypeRegistry;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.ParserUtil;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.misc.ParseCancellationException;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class DefaultKsqlParser implements KsqlParser {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  @VisibleForTesting
  public static final BaseErrorListener ERROR_VALIDATOR = new SyntaxErrorValidator();
  static final BaseErrorListener ERROR_LISTENER = new BaseErrorListener() {
    @Override
    public void syntaxError(
        final Recognizer<?, ?> recognizer,
        final Object offendingSymbol,
        final int line,
        final int charPositionInLine,
        final String message,
        final RecognitionException e
    ) {
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
  };

  @Override
  public List<ParsedStatement> parse(final String sql) {
    try {
      final SqlBaseParser.StatementsContext statementsContext = getParseTree(sql);

      return statementsContext.singleStatement().stream()
          .map(DefaultKsqlParser::parsedStatement)
          .collect(Collectors.toList());

    } catch (final ParsingException e) {
      // ParsingException counts lines starting from 1
      final String failedLine =  sql.split(System.lineSeparator())[e.getLineNumber() - 1];
      throw new ParseFailedException(
          e.getMessage(),
          e.getUnloggedDetails(),
          failedLine,
          e
      );
    } catch (final KsqlStatementException e) {
      throw new ParseFailedException(e.getMessage(), e.getUnloggedMessage(), sql, e);
    } catch (final Exception e) {
      throw new ParseFailedException(e.getMessage(), sql, e);
    }
  }

  public static ParsedStatement parsedStatement(
      final SqlBaseParser.SingleStatementContext statement) {
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

      return PreparedStatement.of(stmt.getUnMaskedStatementText(), root);
    } catch (final ParseFailedException e) {
      if (!e.getSqlStatement().isEmpty()) {
        throw e;
      }
      throw new ParseFailedException(
          e.getRawMessage(), stmt.getMaskedStatementText(), e.getCause());
    } catch (final ParsingException e) {
      throw new ParseFailedException(
          "Failed to prepare statement: " + e.getMessage(),
          "Failed to prepare statement: " + e.getUnloggedDetails(),
          stmt.getMaskedStatementText(),
          e
      );
    } catch (final Exception e) {
      throw new ParseFailedException(
          "Failed to prepare statement: " + e.getMessage(), stmt.getMaskedStatementText(), e);
    }
  }

  public static SqlBaseParser.StatementsContext getParseTree(final String sql) {

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

  private static String getStatementString(
      final SqlBaseParser.SingleStatementContext singleStatementContext) {
    final CharStream charStream = singleStatementContext.start.getInputStream();
    return charStream.getText(Interval.of(
        singleStatementContext.start.getStartIndex(),
        singleStatementContext.stop.getStopIndex()
    ));
  }

  /**
   * checks if the error is a reserved keyword error by checking the message and offendingSymbol
   * @param message the error message
   * @param offendingSymbol the symbol that caused the error
   * @return true if the error is a reserved keyword
   */
  private static boolean isKeywordError(final String message, final String offendingSymbol) {
    final Matcher m = ParserUtil.EXTRANEOUS_INPUT_PATTERN.matcher(message);

    return  m.find() && ParserUtil.isReserved(offendingSymbol);
  }
}
