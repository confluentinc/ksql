package io.confluent.ksql.codegen;

import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.AnalysisContext;
import io.confluent.ksql.analyzer.Analyzer;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.MetaStoreFixture;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class SqlToJavaVisitorTest {

  private static final KsqlParser KSQL_PARSER = new KsqlParser();

  private MetaStore metaStore;
  private Schema schema;
  private CodeGenRunner codeGenRunner;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore();
    schema = SchemaBuilder.struct()
            .field("TEST1.COL0", SchemaBuilder.INT64_SCHEMA)
            .field("TEST1.COL1", SchemaBuilder.STRING_SCHEMA)
            .field("TEST1.COL2", SchemaBuilder.STRING_SCHEMA)
            .field("TEST1.COL3", SchemaBuilder.FLOAT64_SCHEMA);
    codeGenRunner = new CodeGenRunner();
  }

  private Analysis analyzeQuery(String queryStr) {
    List<Statement> statements = KSQL_PARSER.buildAst(queryStr, metaStore);
    // Analyze the query to resolve the references and extract oeprations
    Analysis analysis = new Analysis();
    Analyzer analyzer = new Analyzer(analysis, metaStore);
    analyzer.process(statements.get(0), new AnalysisContext(null));
    return analysis;
  }

  @Test
  public void processBasicJavaMath() throws Exception {


    String simpleQuery = "SELECT col0+col3, col2, col3+10, col0*25, 12*4+2 FROM test1 WHERE col0 > 100;";
    Analysis analysis = analyzeQuery(simpleQuery);

    String javaExpression = new SqlToJavaVisitor().process(analysis.getSelectExpressions().get(0), schema);

    Assert.assertEquals("(TEST1_COL0 + TEST1_COL3)", javaExpression);

  }

}