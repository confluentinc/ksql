package io.confluent.ksql.parser.rewrite;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.parser.*;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.DataSourceExtractor;
import io.confluent.ksql.util.KSQLException;
import io.confluent.ksql.util.Pair;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class KSQLRewriteParser {

  StringBuilder rewrittenQuery = new StringBuilder();

  public List<Pair<Statement, DataSourceExtractor>> buildAST(String sql, MetaStore metaStore) {

    SqlBaseLexer
        sqlBaseLexer =
        new SqlBaseLexer(new CaseInsensitiveStream(new ANTLRInputStream(sql)));

    CommonTokenStream tokenStream = new CommonTokenStream(sqlBaseLexer);

    SqlBaseParser sqlBaseParser = new SqlBaseParser(tokenStream);

    Function<SqlBaseParser, ParserRuleContext> parseFunction = SqlBaseParser::statements;

    ParserRuleContext tree;

    MetaStore tempMetaStore = new MetaStoreImpl();
    for (String dataSourceName : metaStore.getAllDataSources().keySet()) {
      tempMetaStore.putSource(metaStore.getSource(dataSourceName));
    }

    try {
      // first, try parsing with potentially faster SLL mode
      sqlBaseParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
      tree = parseFunction.apply(sqlBaseParser);

      SqlBaseParser.StatementsContext statementsContext = (SqlBaseParser.StatementsContext) tree;
      List<Pair<Statement, DataSourceExtractor>> astNodes = new ArrayList<>();
      for (SqlBaseParser.SingleStatementContext statementContext : statementsContext
          .singleStatement()) {
        DataSourceExtractor dataSourceExtractor = new DataSourceExtractor(tempMetaStore);
        dataSourceExtractor.extractDataSources(statementContext);
        AstBuilder astBuilder = new AstBuilder(dataSourceExtractor);
        Node root = astBuilder.visit(statementContext);
        Statement statement = (Statement) root;

        tempMetaStore.putSource(astBuilder.resultDataSource);

        String sqlStr = SqlFormatterQueryRewrite.formatSql(root);
        System.out.println(sqlStr);

        rewrittenQuery.append(sqlStr).append(";\n");

        astNodes.add(new Pair<>(statement, dataSourceExtractor));
      }
      return astNodes;
    } catch (ParseCancellationException ex) {
      // if we fail, parse with LL mode
      throw new KSQLException(ex.getMessage());
    }
  }

  public String getRewrittenQueryString() {
    return rewrittenQuery.toString();
  }
}
