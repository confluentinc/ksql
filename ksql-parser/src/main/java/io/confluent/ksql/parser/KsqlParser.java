/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.parser;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.SqlBaseParser.SingleStatementContext;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.rewrite.StatementRewriteForStruct;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.DataSourceExtractor;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.misc.ParseCancellationException;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class KsqlParser {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  public static final class ParsedStatement {
    private final String statementText;
    private final SingleStatementContext statement;

    private ParsedStatement(final String statementText, final SingleStatementContext statement) {
      this.statementText = Objects.requireNonNull(statementText, "statementText");
      this.statement = Objects.requireNonNull(statement, "statement");
    }

    public String getStatementText() {
      return statementText;
    }

    public SingleStatementContext getStatement() {
      return statement;
    }
  }

  public static final class PreparedStatement<T extends Statement> {
    private final String statementText;
    private final T statement;

    public PreparedStatement(final String statementText, final T statement) {
      this.statementText = Objects.requireNonNull(statementText, "statementText");
      this.statement = Objects.requireNonNull(statement, "statement");
    }

    public String getStatementText() {
      return statementText;
    }

    public T getStatement() {
      return statement;
    }

    @Override
    public String toString() {
      return statementText;
    }
  }

  public List<PreparedStatement<?>> buildAst(
      final String sql,
      final MetaStore metaStore) {

    return buildAst(sql, metaStore, Function.identity());
  }

  public List<PreparedStatement<?>> buildAst(
      final String sql,
      final MetaStore metaStore,
      final Consumer<? super PreparedStatement<?>> mapper) {

    return buildAst(sql, metaStore, stmt -> true, stmt -> {
      mapper.accept(stmt);
      return stmt;
    });
  }

  public <T> List<T> buildAst(
      final String sql,
      final MetaStore metaStore,
      final Function<? super PreparedStatement<?>, T> mapper) {

    return buildAst(sql, metaStore, stmt -> true, mapper);
  }

  public <T> List<T> buildAst(
      final String sql,
      final MetaStore metaStore,
      final Predicate<ParsedStatement> filter,
      final Function<? super PreparedStatement<?>, T> mapper) {

    return getStatements(sql)
        .stream()
        .filter(filter)
        .map(stmt -> prepareStatement(stmt, metaStore))
        .map(mapper)
        .collect(Collectors.toList());
  }

  public List<ParsedStatement> getStatements(final String sql) {
    try {
      final SqlBaseParser.StatementsContext statementsContext =
          (SqlBaseParser.StatementsContext) getParseTree(sql);

      return statementsContext.singleStatement().stream()
          .map(stmt -> new ParsedStatement(getStatementString(stmt), stmt))
          .collect(Collectors.toList());
    } catch (final Exception e) {
      throw new ParseFailedException(e.getMessage(), sql, e);
    }
  }

  private PreparedStatement<?> prepareStatement(
      final ParsedStatement parsedStatement,
      final MetaStore metaStore) {

    try {
      final DataSourceExtractor dataSourceExtractor = new DataSourceExtractor(metaStore);
      dataSourceExtractor.extractDataSources(parsedStatement.getStatement());

      final AstBuilder astBuilder = new AstBuilder(dataSourceExtractor);
      final Node root = astBuilder.visit(parsedStatement.getStatement());
      Statement statement = (Statement) root;
      if (StatementRewriteForStruct.requiresRewrite(statement)) {
        statement = new StatementRewriteForStruct(statement, dataSourceExtractor)
            .rewriteForStruct();
      }
      return new PreparedStatement<>(parsedStatement.getStatementText(), statement);
    } catch (final ParseFailedException e) {
      if (!e.getSqlStatement().isEmpty()) {
        throw e;
      }
      throw new ParseFailedException(
          e.getRawMessage(), parsedStatement.statementText, e.getCause());
    } catch (final Exception e) {
      throw new ParseFailedException(
          "Failed to prepare statement: " + e.getMessage(), parsedStatement.statementText, e);
    }
  }

  private ParserRuleContext getParseTree(final String sql) {

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
      return parseFunction.apply(sqlBaseParser);
    } catch (final ParseCancellationException ex) {
      // if we fail, parse with LL mode
      tokenStream.seek(0); // rewind input stream
      sqlBaseParser.reset();

      sqlBaseParser.getInterpreter().setPredictionMode(PredictionMode.LL);
      return parseFunction.apply(sqlBaseParser);
    }
  }

  private static final BaseErrorListener ERROR_LISTENER = new BaseErrorListener() {
    @Override
    public void syntaxError(
        final Recognizer<?, ?> recognizer,
        final Object offendingSymbol,
        final int line,
        final int charPositionInLine,
        final String message,
        final RecognitionException e) {
      throw new ParsingException(message, e, line, charPositionInLine);
    }
  };

  private static String getStatementString(
      final SingleStatementContext singleStatementContext
  ) {
    final CharStream charStream = singleStatementContext.start.getInputStream();
    return charStream.getText(Interval.of(
        singleStatementContext.start.getStartIndex(),
        singleStatementContext.stop.getStopIndex()
    ));
  }
}
