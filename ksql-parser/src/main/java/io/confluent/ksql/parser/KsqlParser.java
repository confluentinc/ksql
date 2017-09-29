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

import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.DataSourceExtractor;
import io.confluent.ksql.util.Pair;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;


public class KsqlParser {

  /**
   * Builds an AST from the given query string.
   */
  public List<Statement> buildAst(String sql, MetaStore metaStore) {

    try {
      ParserRuleContext tree = getParseTree(sql);
      SqlBaseParser.StatementsContext statementsContext = (SqlBaseParser.StatementsContext) tree;
      List<Statement> astNodes = new ArrayList<>();
      for (SqlBaseParser.SingleStatementContext statementContext : statementsContext
          .singleStatement()) {
        DataSourceExtractor dataSourceExtractor = new DataSourceExtractor(metaStore);
        dataSourceExtractor.extractDataSources(statementContext);
        Node root = new AstBuilder(dataSourceExtractor).visit(statementContext);
        Statement statement = (Statement) root;

        astNodes.add(statement);
      }
      return astNodes;
    } catch (Exception e) {
      // if we fail, parse with LL mode
      throw new ParseFailedException(e.getMessage(), e);
    }
  }

  public List<SqlBaseParser.SingleStatementContext> getStatements(String sql) {
    try {
      ParserRuleContext tree = getParseTree(sql);
      SqlBaseParser.StatementsContext statementsContext = (SqlBaseParser.StatementsContext) tree;
      return statementsContext.singleStatement();
    } catch (Exception e) {
      throw new ParseFailedException(e.getMessage(), e);
    }
  }


  public Pair<Statement, DataSourceExtractor> prepareStatement(
      SqlBaseParser.SingleStatementContext statementContext, MetaStore metaStore) {
    DataSourceExtractor dataSourceExtractor = new DataSourceExtractor(metaStore);
    dataSourceExtractor.extractDataSources(statementContext);
    AstBuilder astBuilder = new AstBuilder(dataSourceExtractor);
    Node root = astBuilder.visit(statementContext);
    Statement statement = (Statement) root;
    return new Pair<>(statement, dataSourceExtractor);
  }


  private ParserRuleContext getParseTree(String sql) {

    SqlBaseLexer
        sqlBaseLexer =
        new SqlBaseLexer(new CaseInsensitiveStream(new ANTLRInputStream(sql)));
    CommonTokenStream tokenStream = new CommonTokenStream(sqlBaseLexer);
    SqlBaseParser sqlBaseParser = new SqlBaseParser(tokenStream);

    sqlBaseLexer.removeErrorListeners();
    sqlBaseLexer.addErrorListener(ERROR_LISTENER);

    sqlBaseParser.removeErrorListeners();
    sqlBaseParser.addErrorListener(ERROR_LISTENER);

    Function<SqlBaseParser, ParserRuleContext> parseFunction = SqlBaseParser::statements;
    ParserRuleContext tree;
    try {
      // first, try parsing with potentially faster SLL mode
      sqlBaseParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
      tree = parseFunction.apply(sqlBaseParser);
    } catch (ParseCancellationException ex) {
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
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line,
                            int charPositionInLine, String message, RecognitionException e) {
      throw new ParsingException(message, e, line, charPositionInLine);
    }
  };

}
