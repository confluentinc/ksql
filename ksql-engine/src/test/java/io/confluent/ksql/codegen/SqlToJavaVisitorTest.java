package io.confluent.ksql.codegen;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.AnalysisContext;
import io.confluent.ksql.analyzer.Analyzer;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.util.MetaStoreFixture;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class SqlToJavaVisitorTest {

  private static final KsqlParser KSQL_PARSER = new KsqlParser();

  private MetaStore metaStore;
  private Schema schema;
  private InternalFunctionRegistry functionRegistry;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
    functionRegistry = new InternalFunctionRegistry();
    final Schema addressSchema = SchemaBuilder.struct()
        .field("NUMBER",Schema.INT64_SCHEMA)
        .field("STREET", Schema.STRING_SCHEMA)
        .field("CITY", Schema.STRING_SCHEMA)
        .field("STATE", Schema.STRING_SCHEMA)
        .field("ZIPCODE", Schema.INT64_SCHEMA)
        .build();


    schema = SchemaBuilder.struct()
        .field("TEST1.COL0", SchemaBuilder.INT64_SCHEMA)
        .field("TEST1.COL1", SchemaBuilder.STRING_SCHEMA)
        .field("TEST1.COL2", SchemaBuilder.STRING_SCHEMA)
        .field("TEST1.COL3", SchemaBuilder.FLOAT64_SCHEMA)
        .field("TEST1.COL4", SchemaBuilder.array(Schema.FLOAT64_SCHEMA))
        .field("TEST1.COL5", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.FLOAT64_SCHEMA))
        .field("TEST1.COL6", addressSchema)
        .build();
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
  public void shouldProcessBasicJavaMath() {
    String simpleQuery = "SELECT col0+col3, col2, col3+10, col0*25, 12*4+2 FROM test1 WHERE col0 > 100;";
    Analysis analysis = analyzeQuery(simpleQuery);

    String javaExpression = new SqlToJavaVisitor(schema, functionRegistry)
        .process(analysis.getSelectExpressions().get(0));

    assertThat(javaExpression, equalTo("(TEST1_COL0 + TEST1_COL3)"));
  }

  @Test
  public void shouldProcessArrayExpressionCorrectly() throws Exception {

    String simpleQuery = "SELECT col4[0] FROM test1 WHERE col0 > 100;";
    Analysis analysis = analyzeQuery(simpleQuery);

    String javaExpression = new SqlToJavaVisitor(schema, functionRegistry)
        .process(analysis.getSelectExpressions().get(0));

    assertThat(javaExpression,
        equalTo("((Double) ((java.util.List)TEST1_COL4).get((int)(Integer.parseInt(\"0\"))))"));
  }

  @Test
  public void shouldProcessMapExpressionCorrectly() throws Exception {


    String simpleQuery = "SELECT col5['key1'] FROM test1 WHERE col0 > 100;";
    Analysis analysis = analyzeQuery(simpleQuery);

    String javaExpression = new SqlToJavaVisitor(schema, functionRegistry)
        .process(analysis.getSelectExpressions().get(0));

    assertThat(javaExpression, equalTo("((Double) ((java.util.Map)TEST1_COL5).get(\"key1\"))"));
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

  @Test
  public void shouldPostfixFunctionInstancesWithUniqueId() {
    final Analysis analysis = analyzeQuery(
        "SELECT CONCAT(SUBSTRING(col1,1,3),CONCAT('-',SUBSTRING(col1,4,5))) FROM test1;");

    final String javaExpression = new SqlToJavaVisitor(schema, functionRegistry)
        .process(analysis.getSelectExpressions().get(0));

    assertThat(javaExpression, is(
        "((String) CONCAT_0.evaluate("
        + "((String) SUBSTRING_1.evaluate(TEST1_COL1, Integer.parseInt(\"1\"), Integer.parseInt(\"3\"))), "
        + "((String) CONCAT_2.evaluate(\"-\","
        + " ((String) SUBSTRING_3.evaluate(TEST1_COL1, Integer.parseInt(\"4\"), Integer.parseInt(\"5\")))))))"));
  }
}