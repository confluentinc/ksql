package io.confluent.ksql.parser;

import io.confluent.ksql.parser.tree.Node;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.util.function.Function;


public class KSQLParser {


    public Node buildAST(String sql) {
        SqlBaseLexer sqlBaseLexer = new SqlBaseLexer(new ANTLRInputStream(sql));

        CommonTokenStream tokenStream = new CommonTokenStream(sqlBaseLexer);

        SqlBaseParser sqlBaseParser = new SqlBaseParser(tokenStream);

        Function<SqlBaseParser, ParserRuleContext> parseFunction = SqlBaseParser::statements;

        ParserRuleContext tree;
        try {
            // first, try parsing with potentially faster SLL mode
            sqlBaseParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            tree = parseFunction.apply(sqlBaseParser);
            Node root = new AstBuilder().visit(tree);
            return root;
        }
        catch (ParseCancellationException ex) {
            // if we fail, parse with LL mode
            tokenStream.reset(); // rewind input stream
            sqlBaseParser.reset();

            sqlBaseParser.getInterpreter().setPredictionMode(PredictionMode.LL);
            tree = parseFunction.apply(sqlBaseParser);
            Node root = new AstBuilder().visit(tree);
            return root;
        }
    }


    public static void main(String args[]) {

//        String sql = "SELECT COL1, COL2, COL3 FROM TEST WHERE COL1 = 10";

//        String sql = "SELECT time, id OR value, 1 , 2.1, 'hello' INTO testStr FROM orders WHERE COL1 = 10".toUpperCase();
        String sql = "SELECT time INTO testStr FROM orders WHERE COL1 = 10;".toUpperCase();
//        String sql = "SELECT time, id, itemId, units FROM order WHERE id = 100".toUpperCase();

        System.out.println(sql);

        SqlBaseLexer sqlBaseLexer = new SqlBaseLexer(new ANTLRInputStream(sql));

        CommonTokenStream tokenStream = new CommonTokenStream(sqlBaseLexer);

        SqlBaseParser sqlBaseParser = new SqlBaseParser(tokenStream);

        Function<SqlBaseParser, ParserRuleContext> parseFunction = SqlBaseParser::statements;

        ParserRuleContext tree;
        try {
            // first, try parsing with potentially faster SLL mode
            sqlBaseParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            tree = parseFunction.apply(sqlBaseParser);
            Node root = new AstBuilder().visit(tree);
            System.out.println("");
        }
        catch (ParseCancellationException ex) {
            // if we fail, parse with LL mode
            tokenStream.reset(); // rewind input stream
            sqlBaseParser.reset();

            sqlBaseParser.getInterpreter().setPredictionMode(PredictionMode.LL);
            tree = parseFunction.apply(sqlBaseParser);
        }


//        Function<SqlBaseParser, ParserRuleContext> parseFunction = SqlBaseParser::singleStatement;
//
//        ParserRuleContext tree;
//        try {
//            // first, try parsing with potentially faster SLL mode
//            sqlBaseParser.getInterpreter().setPredictionMode(PredictionMode.SLL);
//            tree = parseFunction.apply(sqlBaseParser);
////            Node root = new AstBuilder().visit(tree);
//            ASTNode root = new NewASTBuilder().visit(tree);
//            System.out.println("");
//        }
//        catch (ParseCancellationException ex) {
//            // if we fail, parse with LL mode
//            tokenStream.reset(); // rewind input stream
//            sqlBaseParser.reset();
//
//            sqlBaseParser.getInterpreter().setPredictionMode(PredictionMode.LL);
//            tree = parseFunction.apply(sqlBaseParser);
//        }


        System.out.println("");

    }

}
