package io.confluent.ksql.parser;

import io.confluent.ksql.metastore.MetaStore;
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
    public List<Pair<Statement, DataSourceExtractor>> buildAST(String sql, MetaStore metaStore) {
        SqlBaseLexer sqlBaseLexer = new SqlBaseLexer(new ANTLRInputStream(sql));

        CommonTokenStream tokenStream = new CommonTokenStream(sqlBaseLexer);

        SqlBaseParser sqlBaseParser = new SqlBaseParser(tokenStream);

        Function<SqlBaseParser, ParserRuleContext> parseFunction = SqlBaseParser::statements;

        ParserRuleContext tree;
        try {
            // first, try parsing with potentially faster SLL mode
            sqlBaseParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            tree = parseFunction.apply(sqlBaseParser);

            SqlBaseParser.StatementsContext statementsContext = (SqlBaseParser.StatementsContext) tree;
            List<Pair<Statement, DataSourceExtractor>> astNodes = new ArrayList<>();
            for (SqlBaseParser.SingleStatementContext statementContext: statementsContext.singleStatement()) {
                DataSourceExtractor dataSourceExtractor = new DataSourceExtractor(metaStore);
                dataSourceExtractor.extractDataSources(statementContext);
                Node root = new AstBuilder(dataSourceExtractor).visit(statementContext);
                Statement statement = (Statement) root;
                astNodes.add(new Pair<>(statement, dataSourceExtractor));
            }
            return astNodes;

//            DataSourceExtractor dataSourceExtractor = new DataSourceExtractor(metaStore);
//            dataSourceExtractor.extractDataSources(tree);
//
//            Node root = new AstBuilder(dataSourceExtractor).visit(tree);
//
//            return new Pair<>(root,dataSourceExtractor);
        }
        catch (ParseCancellationException ex) {
            // if we fail, parse with LL mode
            throw new KSQLException(ex.getMessage());
        }
    }

}
