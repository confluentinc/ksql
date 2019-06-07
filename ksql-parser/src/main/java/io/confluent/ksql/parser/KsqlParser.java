/**
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
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.rewrite.StatementRewriteForStruct;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.DataSourceExtractor;
import io.confluent.ksql.util.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;


public class KsqlParser {

  /**
   * Builds an AST from the given query string.
   */
  public List<Statement> buildAst(final String sql, final MetaStore metaStore) {

    try {
      final ParserRuleContext tree = getParseTree(sql);
      final SqlBaseParser.StatementsContext statementsContext =
          (SqlBaseParser.StatementsContext) tree;
      final List<Statement> astNodes = new ArrayList<>();
      for (final SqlBaseParser.SingleStatementContext statementContext : statementsContext
          .singleStatement()) {
        final DataSourceExtractor dataSourceExtractor = new DataSourceExtractor(metaStore);
        dataSourceExtractor.extractDataSources(statementContext);
        final Node root = new AstBuilder(dataSourceExtractor).visit(statementContext);
        Statement statement = (Statement) root;
        if (StatementRewriteForStruct.requiresRewrite(statement)) {
          statement = new StatementRewriteForStruct(statement, dataSourceExtractor)
              .rewriteForStruct();
        }
        astNodes.add(statement);
      }
      return astNodes;
    } catch (final Exception e) {
      // if we fail, parse with LL mode
      throw new ParseFailedException(e.getMessage(), e);
    }
  }

  public List<SqlBaseParser.SingleStatementContext> getStatements(final String sql) {
    try {
      final ParserRuleContext tree = getParseTree(sql);
      final SqlBaseParser.StatementsContext statementsContext =
          (SqlBaseParser.StatementsContext) tree;
      return statementsContext.singleStatement();
    } catch (final Exception e) {
      throw new ParseFailedException(e.getMessage(), e);
    }
  }


  public Pair<Statement, DataSourceExtractor> prepareStatement(
      final SqlBaseParser.SingleStatementContext statementContext, final MetaStore metaStore) {
    final DataSourceExtractor dataSourceExtractor = new DataSourceExtractor(metaStore);
    dataSourceExtractor.extractDataSources(statementContext);
    final AstBuilder astBuilder = new AstBuilder(dataSourceExtractor);
    final Node root = astBuilder.visit(statementContext);
    Statement statement = (Statement) root;
    if (StatementRewriteForStruct.requiresRewrite(statement)) {
      statement = new StatementRewriteForStruct(statement, dataSourceExtractor)
          .rewriteForStruct();
    }
    return new Pair<>(statement, dataSourceExtractor);
  }


  private ParserRuleContext getParseTree(final String sql) {

    final SqlBaseLexer
        sqlBaseLexer =
        new SqlBaseLexer(new CaseInsensitiveStream(new ANTLRInputStream(sql)));
    final CommonTokenStream tokenStream = new CommonTokenStream(sqlBaseLexer);
    final SqlBaseParser sqlBaseParser = new SqlBaseParser(tokenStream);

    sqlBaseLexer.removeErrorListeners();
    sqlBaseLexer.addErrorListener(ERROR_LISTENER);

    sqlBaseParser.removeErrorListeners();
    sqlBaseParser.addErrorListener(ERROR_LISTENER);

    final Function<SqlBaseParser, ParserRuleContext> parseFunction = SqlBaseParser::statements;
    ParserRuleContext tree;
    try {
      // first, try parsing with potentially faster SLL mode
      sqlBaseParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
      tree = parseFunction.apply(sqlBaseParser);
    } catch (final ParseCancellationException ex) {
      // if we fail, parse with LL mode
      tokenStream.reset(); // rewind input stream
      sqlBaseParser.reset();

      sqlBaseParser.getInterpreter().setPredictionMode(PredictionMode.LL);
      tree = parseFunction.apply(sqlBaseParser);
    }

    return tree;
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

}
