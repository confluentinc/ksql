package io.confluent.ksql.codegen;

import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.AnalysisContext;
import io.confluent.ksql.analyzer.Analyzer;
import io.confluent.ksql.function.FunctionRegistry;
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class SqlToJavaVisitorTest {

  private static final KsqlParser KSQL_PARSER = new KsqlParser();

  private MetaStore metaStore;
  private Schema schema;
  private CodeGenRunner codeGenRunner;
  private FunctionRegistry functionRegistry;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore();
    functionRegistry = new FunctionRegistry();
    schema = SchemaBuilder.struct()
            .field("TEST1.COL0", SchemaBuilder.INT64_SCHEMA)
            .field("TEST1.COL1", SchemaBuilder.STRING_SCHEMA)
            .field("TEST1.COL2", SchemaBuilder.STRING_SCHEMA)
            .field("TEST1.COL3", SchemaBuilder.FLOAT64_SCHEMA);
    codeGenRunner = new CodeGenRunner(schema, functionRegistry);
  }

  private Analysis analyzeQuery(String queryStr) {
    List<Statement> statements = KSQL_PARSER.buildAst(queryStr, metaStore);
    // Analyze the query to resolve the references and extract oeprations
    Analysis analysis = new Analysis();
    Analyzer analyzer = new Analyzer("sqlExpression", analysis, metaStore);
    analyzer.process(statements.get(0), new AnalysisContext(null));
    return analysis;
  }

  @Test
  public void processBasicJavaMath() throws Exception {


    String simpleQuery = "SELECT col0+col3, col2, col3+10, col0*25, 12*4+2 FROM test1 WHERE col0 > 100;";
    Analysis analysis = analyzeQuery(simpleQuery);

    String javaExpression = new SqlToJavaVisitor(schema, functionRegistry)
        .process(analysis.getSelectExpressions().get(0));

    assertThat(javaExpression, equalTo("(TEST1_COL0 + TEST1_COL3)"));

  }

  @Test
  public void shouldCreateCorrectCastJavaExpression() throws Exception {


    String simpleQuery = "SELECT cast(col0 AS INTEGER), cast(col3 as BIGINT), cast(col3 as "
                         + "varchar) FROM "
                         + "test1 WHERE "
                         + "col0 > 100;";
    Analysis analysis = analyzeQuery(simpleQuery);

    String javaExpression0 = new SqlToJavaVisitor(schema, functionRegistry)
        .process(analysis.getSelectExpressions().get(0));
    String javaExpression1 = new SqlToJavaVisitor(schema, functionRegistry)
        .process(analysis.getSelectExpressions().get(1));
    String javaExpression2 = new SqlToJavaVisitor(schema, functionRegistry)
        .process(analysis.getSelectExpressions().get(2));

    assertThat(javaExpression0, equalTo("(new Long(TEST1_COL0).intValue())"));
    assertThat(javaExpression1, equalTo("(new Double(TEST1_COL3).longValue())"));
    assertThat(javaExpression2, equalTo("String.valueOf(TEST1_COL3)"));
  }

}