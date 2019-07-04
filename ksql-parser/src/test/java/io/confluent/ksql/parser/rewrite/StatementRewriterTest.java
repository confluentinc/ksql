/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.parser.rewrite;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;
import static org.mockito.Mockito.mock;

import com.google.common.collect.Iterables;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParserTestUtil;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.util.MetaStoreFixture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StatementRewriterTest {

  private MetaStore metaStore;
  private StatementRewriter statementRewriter;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(mock(FunctionRegistry.class));
    statementRewriter = new StatementRewriter();
  }
  
  @Test
  public void testProjection() {
    final String queryStr = "SELECT col0, col2, col3 FROM test1;";
    final Statement statement = parse(queryStr);

    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));

    final Query query = (Query) rewrittenStatement;
    assertThat(query.getSelect().getSelectItems().size() , equalTo(3));
    assertThat(query.getSelect().getSelectItems().get(0), instanceOf(SingleColumn.class));
    final SingleColumn column0 = (SingleColumn)query.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias(), equalTo("COL0"));
    assertThat(column0.getExpression().toString(), equalTo("TEST1.COL0"));
  }

  @Test
  public void testProjectionWithArrayMap() {
    final String queryStr = "SELECT col0, col2, col3, col4[0], col5['key1'] FROM test1;";
    final Statement statement = parse(queryStr);

    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));

    final Query query = (Query) rewrittenStatement;
    assertThat(query.getSelect().getSelectItems()
        .size(), equalTo(5));
    assertThat(query.getSelect().getSelectItems().get(0), instanceOf(SingleColumn.class));
    final SingleColumn column0 = (SingleColumn)query.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias(), equalTo("COL0"));
    assertThat(column0.getExpression().toString(), equalTo("TEST1.COL0"));

    final SingleColumn column3 = (SingleColumn)query.getSelect().getSelectItems().get(3);
    final SingleColumn column4 = (SingleColumn)query.getSelect().getSelectItems().get(4);
    assertThat(column3.getExpression().toString(), equalTo("TEST1.COL4[0]"));
    assertThat(column4.getExpression().toString(), equalTo("TEST1.COL5['key1']"));
  }

  @Test
  public void testProjectFilter() {
    final String queryStr = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final Statement statement = parse(queryStr);
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));

    final Query query = (Query) rewrittenStatement;

    assertThat(query.getWhere().get(), instanceOf(ComparisonExpression.class));
    final ComparisonExpression comparisonExpression = (ComparisonExpression)query.getWhere().get();
    assertThat(comparisonExpression.toString(), equalTo("(TEST1.COL0 > 100)"));
    assertThat(query.getSelect().getSelectItems().size(), equalTo(3));

  }

  @Test
  public void testBinaryExpression() {
    final String queryStr = "SELECT col0+10, col2, col3-col1 FROM test1;";
    final Statement statement = parse(queryStr);
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));

    final Query query = (Query) rewrittenStatement;
    final SingleColumn column0 = (SingleColumn)query.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias(), equalTo("KSQL_COL_0"));
    assertThat(column0.getExpression().toString(), equalTo("(TEST1.COL0 + 10)"));
  }

  @Test
  public void testBooleanExpression() {
    final String queryStr = "SELECT col0 = 10, col2, col3 > col1 FROM test1;";
    final Statement statement = parse(queryStr);
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));

    final Query query = (Query) rewrittenStatement;
    final SingleColumn column0 = (SingleColumn)query.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias(), equalTo("KSQL_COL_0"));
    assertThat(column0.getExpression().toString(), equalTo("(TEST1.COL0 = 10)"));
    assertThat(column0.getExpression(), instanceOf(ComparisonExpression.class));
  }

  @Test
  public void testLiterals() {
    final String queryStr = "SELECT 10, col2, 'test', 2.5, true, -5 FROM test1;";
    final Statement statement = parse(queryStr);
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));

    final Query query = (Query) rewrittenStatement;
    final SingleColumn column0 = (SingleColumn)query.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias(), equalTo("KSQL_COL_0"));
    assertThat(column0.getExpression().toString(), equalTo("10"));

    final SingleColumn column1 = (SingleColumn)query.getSelect().getSelectItems().get(1);
    assertThat(column1.getAlias(), equalTo("COL2"));
    assertThat(column1.getExpression().toString(), equalTo("TEST1.COL2"));

    final SingleColumn column2 = (SingleColumn)query.getSelect().getSelectItems().get(2);
    assertThat(column2.getAlias(), equalTo("KSQL_COL_2"));
    assertThat(column2.getExpression().toString(), equalTo("'test'"));

    final SingleColumn column3 = (SingleColumn)query.getSelect().getSelectItems().get(3);
    assertThat(column3.getAlias(), equalTo("KSQL_COL_3"));
    assertThat(column3.getExpression().toString(), equalTo("2.5"));

    final SingleColumn column4 = (SingleColumn)query.getSelect().getSelectItems().get(4);
    assertThat(column4.getAlias(), equalTo("KSQL_COL_4"));
    assertThat(column4.getExpression().toString(), equalTo("true"));

    final SingleColumn column5 = (SingleColumn)query.getSelect().getSelectItems().get(5);
    assertThat(column5.getAlias(), equalTo("KSQL_COL_5"));
    assertThat(column5.getExpression().toString(), equalTo("-5"));
  }

  @Test
  public void testBooleanLogicalExpression() {
    final String queryStr =
        "SELECT 10, col2, 'test', 2.5, true, -5 FROM test1 WHERE col1 = 10 AND col2 LIKE 'val' OR col4 > 2.6 ;";
    final Statement statement = parse(queryStr);
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));

    final Query query = (Query) rewrittenStatement;
    final SingleColumn column0 = (SingleColumn)query.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias(), equalTo("KSQL_COL_0"));
    assertThat(column0.getExpression().toString(), equalTo("10"));

    final SingleColumn column1 = (SingleColumn)query.getSelect().getSelectItems().get(1);
    assertThat(column1.getAlias(), equalTo("COL2"));
    assertThat(column1.getExpression().toString(), equalTo("TEST1.COL2"));

    final SingleColumn column2 = (SingleColumn)query.getSelect().getSelectItems().get(2);
    assertThat(column2.getAlias(), equalTo("KSQL_COL_2"));
    assertThat(column2.getExpression().toString(), equalTo("'test'"));

  }

  @Test
  public void testSimpleLeftJoin() {
    final String queryStr =
        "SELECT t1.col1, t2.col1, t2.col4, col5, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON "
            + "t1.col1 = t2.col1;";
    final Statement statement = parse(queryStr);
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));

    final Query query = (Query) rewrittenStatement;
    assertThat(query.getFrom(), instanceOf(Join.class));
    final Join join = (Join) query.getFrom();
    assertThat(join.getType().toString(), equalTo("LEFT"));

    assertThat(((AliasedRelation)join.getLeft()).getAlias(), equalTo("T1"));
    assertThat(((AliasedRelation)join.getRight()).getAlias(), equalTo("T2"));

  }

  @Test
  public void testLeftJoinWithFilter() {
    final String queryStr =
        "SELECT t1.col1, t2.col1, t2.col4, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON t1.col1 = "
            + "t2.col1 WHERE t2.col2 = 'test';";
    final Statement statement = parse(queryStr);
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));

    final Query query = (Query) rewrittenStatement;
    assertThat(query.getFrom(), instanceOf(Join.class));
    final Join join = (Join) query.getFrom();
    assertThat(join.getType().toString(), equalTo("LEFT"));

    assertThat(((AliasedRelation)join.getLeft()).getAlias(), equalTo("T1"));
    assertThat(((AliasedRelation)join.getRight()).getAlias(), equalTo("T2"));

    assertThat(query.getWhere().get().toString(), equalTo("(T2.COL2 = 'test')"));
  }

  @Test
  public void testSelectAll() {
    // Given:
    final Query original = parse("SELECT * FROM test1 t1;");

    // When:
    final Node rewrittenStatement = statementRewriter.process(original, null);

    // Then:
    assertThat(rewrittenStatement, is(instanceOf(Query.class)));

    final Query query = (Query) rewrittenStatement;
    assertThat(query.getSelect(), is(original.getSelect()));
  }

  @Test
  public void testSelectAllJoin() {
    // Given:
    final Query original = parse(
        "SELECT * FROM test1 t1 LEFT JOIN test2 t2 ON t1.col1 = t2.col1 "
            + "WHERE t2.col2 = 'test';");

    // When:
    final Node rewrittenStatement = statementRewriter.process(original, null);

    // Then:
    assertThat(rewrittenStatement, instanceOf(Query.class));

    final Query query = (Query) rewrittenStatement;
    assertThat(query.getSelect(), is(original.getSelect()));

    assertThat(query.getFrom(), instanceOf(Join.class));
    final Join join = (Join) query.getFrom();
    assertThat(((AliasedRelation) join.getLeft()).getAlias(), is("T1"));
    assertThat(((AliasedRelation) join.getRight()).getAlias(), is("T2"));
  }

  @Test
  public void testUDF() {
    final String queryStr = "SELECT lcase(col1), concat(col2,'hello'), floor(abs(col3)) FROM test1 t1;";
    final Statement statement = parse(queryStr);

    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));

    final Query query = (Query) rewrittenStatement;

    final SingleColumn column0 = (SingleColumn)query.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias(), equalTo("KSQL_COL_0"));
    assertThat(column0.getExpression().toString(), equalTo("LCASE(T1.COL1)"));

    final SingleColumn column1 = (SingleColumn)query.getSelect().getSelectItems().get(1);
    assertThat(column1.getAlias(), equalTo("KSQL_COL_1"));
    assertThat(column1.getExpression().toString(), equalTo("CONCAT(T1.COL2, 'hello')"));

    final SingleColumn column2 = (SingleColumn)query.getSelect().getSelectItems().get(2);
    assertThat(column2.getAlias(), equalTo("KSQL_COL_2"));
    assertThat(column2.getExpression().toString(), equalTo("FLOOR(ABS(T1.COL3))"));
  }

  @Test
  public void testCreateStreamWithTopic() {
    final String queryStr =
        "CREATE STREAM orders (ordertime bigint, orderid varchar, itemid varchar, orderunits "
            + "double) WITH (kafka_topic = 'foo', value_format = 'json', key='ordertime');";
    final Statement statement = parse(queryStr);

    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(CreateStream.class));
    final CreateStream createStream = (CreateStream)rewrittenStatement;
    assertThat(createStream.getName().toString(), equalTo("ORDERS"));
    assertThat(Iterables.size(createStream.getElements()), equalTo(4));
    assertThat(Iterables.get(createStream.getElements(), 0).getName(), equalTo("ORDERTIME"));
  }

  @Test
  public void testCreateStreamWithTopicWithStruct() {
    final String queryStr =
        "CREATE STREAM orders (ordertime bigint, orderid varchar, itemid varchar, orderunits "
            + "double, arraycol array<double>, mapcol map<varchar, double>, "
            + "order_address STRUCT< number VARCHAR, street VARCHAR, zip INTEGER, city "
            + "VARCHAR, state VARCHAR >) WITH (kafka_topic='foo', value_format='json', key='ordertime');";
    final Statement statement = parse(queryStr);

    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(CreateStream.class));
    final CreateStream createStream = (CreateStream)rewrittenStatement;
    assertThat(createStream.getName().toString().toUpperCase(), equalTo("ORDERS"));
    assertThat(Iterables.size(createStream.getElements()), equalTo(7));
    assertThat(Iterables.get(createStream.getElements(), 0).getName().toLowerCase(), equalTo("ordertime"));
    assertThat(Iterables.get(createStream.getElements(), 6).getType().getSqlType().baseType(), equalTo(SqlBaseType.STRUCT));
    final SqlStruct struct = (SqlStruct) Iterables.get(createStream.getElements(), 6).getType().getSqlType();
    assertThat(struct.getFields(), hasSize(5));
    assertThat(struct.getFields().get(0).getType().baseType(), equalTo(SqlBaseType.STRING));
  }

  @Test
  public void testCreateStream() {
    final String queryStr =
        "CREATE STREAM orders "
            + "(ordertime bigint, orderid varchar, itemid varchar, orderunits double) "
            + "WITH (value_format = 'avro',kafka_topic='orders_topic');";
    final Statement statement = parse(queryStr);

    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(CreateStream.class));
    final CreateStream createStream = (CreateStream)rewrittenStatement;

    assertThat(createStream.getName().toString(), equalTo("ORDERS"));
    assertThat(Iterables.size(createStream.getElements()), equalTo(4));
    assertThat(Iterables.get(createStream.getElements(), 0).getName(), equalTo("ORDERTIME"));
    assertThat(createStream.getProperties().getKafkaTopic(), equalTo("orders_topic"));
    assertThat(createStream.getProperties().getValueFormat(), equalTo(Format.AVRO));
  }

  @Test
  public void testCreateTableWithTopic() {
    final String queryStr =
        "CREATE TABLE users (usertime bigint, userid varchar, regionid varchar, gender varchar) "
            + "WITH (kafka_topic='foo', value_format='json', key='userid');";
    final Statement statement = parse(queryStr);
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);
    assertThat(rewrittenStatement, is(instanceOf(CreateTable.class)));
    final CreateTable createTable = (CreateTable)rewrittenStatement;
    assertThat(createTable.getName().toString(), equalTo("USERS"));
    assertThat(Iterables.size(createTable.getElements()), equalTo(4));
    assertThat(Iterables.get(createTable.getElements(), 0).getName(), equalTo("USERTIME"));
  }

  @Test
  public void testCreateTable() {
    final String queryStr =
        "CREATE TABLE users (usertime bigint, userid varchar, regionid varchar, gender varchar) "
            + "WITH (kafka_topic = 'users_topic', value_format='json', key = 'userid');";
    final Statement statement = parse(queryStr);
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);
    assertThat(rewrittenStatement, is(instanceOf(CreateTable.class)));
    final CreateTable createTable = (CreateTable)rewrittenStatement;
    assertThat(createTable.getName().toString(), equalTo("USERS"));
    assertThat(Iterables.size(createTable.getElements()), equalTo(4));
    assertThat(Iterables.get(createTable.getElements(), 0).getName(), equalTo("USERTIME"));
    assertThat(createTable.getProperties().getKafkaTopic(), equalTo("users_topic"));
    assertThat(createTable.getProperties().getValueFormat(), equalTo(Format.JSON));
  }

  @Test
  public void testCreateStreamAsSelect() {
    // Given:
    final CreateStreamAsSelect original = parse("CREATE STREAM bigorders_json "
        + "WITH (value_format = 'json', kafka_topic='bigorders_topic') "
        + "AS SELECT * FROM orders "
        + "WHERE orderunits > 5;");

    final Statement rewrittenStatement = (Statement) statementRewriter.process(original, null);

    assertThat(rewrittenStatement, is(instanceOf(CreateStreamAsSelect.class)));
    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect)rewrittenStatement;

    assertThat(createStreamAsSelect.getName().toString(), equalTo("BIGORDERS_JSON"));

    final Query query = createStreamAsSelect.getQuery();
    assertThat(query.getSelect(), is(original.getQuery().getSelect()));
    assertThat(query.getWhere().get().toString(), equalTo("(ORDERS.ORDERUNITS > 5)"));
    assertThat(((AliasedRelation)query.getFrom()).getAlias(), equalTo("ORDERS"));
  }

  @Test
  public void testSelectTumblingWindow() {

    final String queryStr =
        "select itemid, sum(orderunits) from orders window TUMBLING ( size 30 second) where orderunits > 5 group by itemid;";
    final Statement statement = parse(queryStr);

    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));
    final Query query = (Query) rewrittenStatement;
    assertThat(query.getSelect().getSelectItems().size(), equalTo(2));
    assertThat(query.getWhere().get().toString(), equalTo("(ORDERS.ORDERUNITS > 5)"));
    assertThat(((AliasedRelation)query.getFrom()).getAlias(), equalTo("ORDERS"));
    Assert.assertTrue( query.getWindow().isPresent());
    assertThat(query
        .getWindow().get().toString(), equalTo(" WINDOW STREAMWINDOW  TUMBLING ( SIZE 30 SECONDS ) "));
  }

  @Test
  public void testSelectHoppingWindow() {

    final String queryStr =
        "select itemid, sum(orderunits) from orders window HOPPING ( size 30 second, advance by 5"
            + " seconds) "
            + "where "
            + "orderunits"
            + " > 5 group by itemid;";
    final Statement statement = parse(queryStr);

    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));
    final Query query = (Query) rewrittenStatement;
    assertThat(query.getSelect().getSelectItems().size(), equalTo(2));
    assertThat(query.getWhere().get().toString(), equalTo("(ORDERS.ORDERUNITS > 5)"));
    assertThat(((AliasedRelation)query.getFrom()).getAlias().toUpperCase(), equalTo("ORDERS"));
    assertThat("window expression isn't present", query
        .getWindow().isPresent());
    assertThat(query.getWindow().get().toString().toUpperCase(),
        equalTo(" WINDOW STREAMWINDOW  HOPPING ( SIZE 30 SECONDS , ADVANCE BY 5 SECONDS ) "));
  }


  @Test
  public void testSelectSessionWindow() {

   final String queryStr =
        "select itemid, sum(orderunits) from orders window SESSION ( 30 second) where "
            + "orderunits > 5 group by itemid;";
    final Statement statement = parse(queryStr);

    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));
    final Query query = (Query) rewrittenStatement;
    assertThat(query.getSelect().getSelectItems().size(), equalTo(2));
    assertThat(query.getWhere().get().toString(), equalTo("(ORDERS.ORDERUNITS > 5)"));
    assertThat(((AliasedRelation)query.getFrom()).getAlias(), equalTo("ORDERS"));
    Assert.assertTrue( query.getWindow().isPresent());
    assertThat(query
        .getWindow().get().toString(), equalTo(" WINDOW STREAMWINDOW  SESSION "
        + "( 30 SECONDS ) "));
  }


  @Test
  public void testSelectSinkProperties() {
    final String simpleQuery = "create stream s1 with (timestamp='orderid', partitions = 3) as select "
        + "col1, col2"
        + " from orders where col2 is null and col3 is not null or (col3*col2 = "
        + "12);";
    final Statement statement = parse(simpleQuery);

    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(CreateStreamAsSelect.class));
    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) rewrittenStatement;
    final Query query = createStreamAsSelect.getQuery();
    assertThat(query.getWhere().toString(), equalTo("Optional[(((ORDERS.COL2 IS NULL) AND (ORDERS.COL3 IS NOT NULL)) OR ((ORDERS.COL3 * ORDERS.COL2) = 12))]"));
  }

  @Test
  public void testInsertInto() {
    final String insertIntoString = "INSERT INTO test0 "
        + "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final Statement statement = parse(insertIntoString);

    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(InsertInto.class));
    final InsertInto insertInto = (InsertInto) rewrittenStatement;
    assertThat(insertInto.getTarget().toString(), equalTo("TEST0"));
    final Query query = insertInto.getQuery();
    assertThat(query.getSelect().getSelectItems().size(), equalTo(3));
    assertThat(query.getFrom(), not(nullValue()));
    assertThat(query.getWhere().isPresent(), equalTo(true));
    assertThat(query.getWhere().get(),  instanceOf(ComparisonExpression.class));
    final ComparisonExpression comparisonExpression = (ComparisonExpression)query.getWhere().get();
    assertThat(comparisonExpression.getType().getValue(), equalTo(">"));
  }

  private <T extends Statement> T parse(final String sql) {
    return KsqlParserTestUtil.<T>buildSingleAst(sql, metaStore).getStatement();
  }
}