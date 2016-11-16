package io.confluent.ksql.parser;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.rewrite.SqlFormatterQueryRewrite;
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


public class KSQLParser {


    /**
     * Builds an AST from the given query string.
     *
     * @param sql
     * @return
     */
    public List<Statement> buildAST(String sql, MetaStore metaStore) {

        SqlBaseLexer sqlBaseLexer = new SqlBaseLexer(new CaseInsensitiveStream(new ANTLRInputStream(sql)));

        CommonTokenStream tokenStream = new CommonTokenStream(sqlBaseLexer);

        SqlBaseParser sqlBaseParser = new SqlBaseParser(tokenStream);

        Function<SqlBaseParser, ParserRuleContext> parseFunction = SqlBaseParser::statements;

        ParserRuleContext tree;
        try {
            // first, try parsing with potentially faster SLL mode
            sqlBaseParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            tree = parseFunction.apply(sqlBaseParser);

            SqlBaseParser.StatementsContext statementsContext = (SqlBaseParser.StatementsContext) tree;
            List<Statement> astNodes = new ArrayList<>();
            for (SqlBaseParser.SingleStatementContext statementContext: statementsContext.singleStatement()) {
                DataSourceExtractor dataSourceExtractor = new DataSourceExtractor(metaStore);
                dataSourceExtractor.extractDataSources(statementContext);
                Node root = new AstBuilder(dataSourceExtractor).visit(statementContext);
                Statement statement = (Statement) root;

                astNodes.add(statement);
            }
            return astNodes;
        }
        catch (ParseCancellationException ex) {
            // if we fail, parse with LL mode
            throw new KSQLException(ex.getMessage());
        }
    }

    public List<SqlBaseParser.SingleStatementContext> getStatements(String sql) {
        SqlBaseLexer sqlBaseLexer = new SqlBaseLexer(new CaseInsensitiveStream(new ANTLRInputStream(sql)));

        CommonTokenStream tokenStream = new CommonTokenStream(sqlBaseLexer);

        SqlBaseParser sqlBaseParser = new SqlBaseParser(tokenStream);

        Function<SqlBaseParser, ParserRuleContext> parseFunction = SqlBaseParser::statements;

        // first, try parsing with potentially faster SLL mode
        sqlBaseParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
        ParserRuleContext tree = parseFunction.apply(sqlBaseParser);

        SqlBaseParser.StatementsContext statementsContext = (SqlBaseParser.StatementsContext) tree;
        return statementsContext.singleStatement();
    }


    public Pair<Statement, DataSourceExtractor> prepareStatement(SqlBaseParser.SingleStatementContext statementContext, MetaStore metaStore) {
        DataSourceExtractor dataSourceExtractor = new DataSourceExtractor(metaStore);
        dataSourceExtractor.extractDataSources(statementContext);
        AstBuilder astBuilder = new AstBuilder(dataSourceExtractor);
        Node root = astBuilder.visit(statementContext);
        Statement statement = (Statement) root;
        metaStore.putSource(astBuilder.resultDataSource);
        return new Pair<>(statement, dataSourceExtractor);
    }

}
