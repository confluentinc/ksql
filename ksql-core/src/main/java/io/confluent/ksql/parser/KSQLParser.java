package io.confluent.ksql.parser;

import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.util.DataSourceExtractor;
import io.confluent.ksql.util.KSQLException;
import io.confluent.ksql.util.Pair;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.util.function.Function;


public class KSQLParser {


    /**
     * Builds an AST from the given query string.
     *
     * @param sql
     * @return
     */
    public Pair<Node, DataSourceExtractor> buildAST(String sql, MetaStore metaStore) {
        SqlBaseLexer sqlBaseLexer = new SqlBaseLexer(new ANTLRInputStream(sql));

        CommonTokenStream tokenStream = new CommonTokenStream(sqlBaseLexer);

        SqlBaseParser sqlBaseParser = new SqlBaseParser(tokenStream);

        Function<SqlBaseParser, ParserRuleContext> parseFunction = SqlBaseParser::statements;

        ParserRuleContext tree;
        try {
            // first, try parsing with potentially faster SLL mode
            sqlBaseParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            tree = parseFunction.apply(sqlBaseParser);

            DataSourceExtractor dataSourceExtractor = new DataSourceExtractor(metaStore);
            dataSourceExtractor.extractDataSources(tree);

            Node root = new AstBuilder(dataSourceExtractor).visit(tree);

            return new Pair<>(root,dataSourceExtractor);
        }
        catch (ParseCancellationException ex) {
            // if we fail, parse with LL mode
            throw new KSQLException(ex.getMessage());
        }
    }

}
