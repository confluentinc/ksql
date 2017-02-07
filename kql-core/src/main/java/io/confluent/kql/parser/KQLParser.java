/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser;

import io.confluent.kql.metastore.MetaStore;
import io.confluent.kql.parser.tree.Node;
import io.confluent.kql.parser.tree.Statement;
import io.confluent.kql.util.DataSourceExtractor;
import io.confluent.kql.util.KQLException;
import io.confluent.kql.util.Pair;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;


public class KQLParser {


  /**
   * Builds an AST from the given query string.
   */
  public List<Statement> buildAST(String sql, MetaStore metaStore) {

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
    } catch (ParseCancellationException ex) {
      // if we fail, parse with LL mode
      throw new KQLException(ex.getMessage());
    }
  }

  public List<SqlBaseParser.SingleStatementContext> getStatements(String sql) {

    ParserRuleContext tree = getParseTree(sql);
    SqlBaseParser.StatementsContext statementsContext = (SqlBaseParser.StatementsContext) tree;
    return statementsContext.singleStatement();
  }


  public Pair<Statement, DataSourceExtractor> prepareStatement(
      SqlBaseParser.SingleStatementContext statementContext, MetaStore metaStore) {
    DataSourceExtractor dataSourceExtractor = new DataSourceExtractor(metaStore);
    dataSourceExtractor.extractDataSources(statementContext);
    AstBuilder astBuilder = new AstBuilder(dataSourceExtractor);
    Node root = astBuilder.visit(statementContext);
    Statement statement = (Statement) root;
//    metaStore.putSource(astBuilder.resultDataSource);
    return new Pair<>(statement, dataSourceExtractor);
  }


  private ParserRuleContext getParseTree(String sql) {

    SqlBaseLexer
        sqlBaseLexer =
        new SqlBaseLexer(new CaseInsensitiveStream(new ANTLRInputStream(sql)));
    CommonTokenStream tokenStream = new CommonTokenStream(sqlBaseLexer);
    SqlBaseParser sqlBaseParser = new SqlBaseParser(tokenStream);
    sqlBaseParser.setErrorHandler(new KQLParserErrorStrategy());
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
}
