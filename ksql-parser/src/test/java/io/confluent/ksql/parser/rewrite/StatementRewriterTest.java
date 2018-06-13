/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.parser.rewrite;

import io.confluent.ksql.function.TestFunctionRegistry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Struct;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.util.MetaStoreFixture;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;

public class StatementRewriterTest {

  private static final KsqlParser KSQL_PARSER = new KsqlParser();

  private MetaStore metaStore;

  @Before
  public void init() {

    metaStore = MetaStoreFixture.getNewMetaStore(new TestFunctionRegistry());
  }
  
  @Test
  public void testProjection() {
    final String queryStr = "SELECT col0, col2, col3 FROM test1;";
    final Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);

    final StatementRewriter statementRewriter = new StatementRewriter();
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));

    final Query query = (Query) rewrittenStatement;
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    final QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    assertThat(querySpecification.getSelect().getSelectItems().size() , equalTo(3));
    assertThat(querySpecification.getSelect().getSelectItems().get(0), instanceOf(SingleColumn.class));
    final SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias().get(), equalTo("COL0"));
    assertThat(column0.getExpression().toString(), equalTo("TEST1.COL0"));
  }

  @Test
  public void testProjectionWithArrayMap() {
    final String queryStr = "SELECT col0, col2, col3, col4[0], col5['key1'] FROM test1;";
    final Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);

    final StatementRewriter statementRewriter = new StatementRewriter();
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));

    final Query query = (Query) rewrittenStatement;
    assertThat("testProjectionWithArrayMap fails", query.getQueryBody(), instanceOf(QuerySpecification.class));
    final QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    assertThat("testProjectionWithArrayMap fails", querySpecification.getSelect().getSelectItems()
        .size(), equalTo(5));
    assertThat("testProjectionWithArrayMap fails", querySpecification.getSelect().getSelectItems().get(0) instanceof SingleColumn);
    final SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
    assertThat("testProjectionWithArrayMap fails", column0.getAlias().get(), equalTo("COL0"));
    assertThat("testProjectionWithArrayMap fails", column0.getExpression().toString(), equalTo("TEST1.COL0"));

    final SingleColumn column3 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(3);
    final SingleColumn column4 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(4);
    assertThat("testProjectionWithArrayMap fails", column3.getExpression().toString(), equalTo("TEST1.COL4[0]"));
    assertThat("testProjectionWithArrayMap fails", column4.getExpression().toString(), equalTo("TEST1.COL5['key1']"));
  }

  @Test
  public void testProjectFilter() {
    final String queryStr = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    final StatementRewriter statementRewriter = new StatementRewriter();
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));

    final Query query = (Query) rewrittenStatement;
    assertThat("testProjectFilter fails", query.getQueryBody(), instanceOf(QuerySpecification.class));
    final QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();

    assertThat("testProjectFilter fails", querySpecification.getWhere().get(), instanceOf(ComparisonExpression.class));
    final ComparisonExpression comparisonExpression = (ComparisonExpression)querySpecification.getWhere().get();
    assertThat("testProjectFilter fails", comparisonExpression.toString(), equalTo("(TEST1.COL0 > 100)"));
    assertThat("testProjectFilter fails", querySpecification.getSelect().getSelectItems().size(), equalTo(3));

  }

  @Test
  public void testBinaryExpression() {
    final String queryStr = "SELECT col0+10, col2, col3-col1 FROM test1;";
    final Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    final StatementRewriter statementRewriter = new StatementRewriter();
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));

    final Query query = (Query) rewrittenStatement;
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    final QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    final SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias().get(), equalTo("KSQL_COL_0"));
    assertThat(column0.getExpression().toString(), equalTo("(TEST1.COL0 + 10)"));
  }

  @Test
  public void testBooleanExpression() {
    final String queryStr = "SELECT col0 = 10, col2, col3 > col1 FROM test1;";
    final Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    final StatementRewriter statementRewriter = new StatementRewriter();
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));

    final Query query = (Query) rewrittenStatement;
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    final QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    final SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias().get(), equalTo("KSQL_COL_0"));
    assertThat(column0.getExpression().toString(), equalTo("(TEST1.COL0 = 10)"));
    assertThat(column0.getExpression(), instanceOf(ComparisonExpression.class));
  }

  @Test
  public void testLiterals() {
    final String queryStr = "SELECT 10, col2, 'test', 2.5, true, -5 FROM test1;";
    final Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    final StatementRewriter statementRewriter = new StatementRewriter();
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));

    final Query query = (Query) rewrittenStatement;
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    final QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    final SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias().get(), equalTo("KSQL_COL_0"));
    assertThat(column0.getExpression().toString(), equalTo("10"));

    final SingleColumn column1 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(1);
    assertThat(column1.getAlias().get(), equalTo("COL2"));
    assertThat(column1.getExpression().toString(), equalTo("TEST1.COL2"));

    final SingleColumn column2 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(2);
    assertThat(column2.getAlias().get(), equalTo("KSQL_COL_2"));
    assertThat(column2.getExpression().toString(), equalTo("'test'"));

    final SingleColumn column3 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(3);
    assertThat(column3.getAlias().get(), equalTo("KSQL_COL_3"));
    assertThat(column3.getExpression().toString(), equalTo("2.5"));

    final SingleColumn column4 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(4);
    assertThat(column4.getAlias().get(), equalTo("KSQL_COL_4"));
    assertThat(column4.getExpression().toString(), equalTo("true"));

    final SingleColumn column5 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(5);
    assertThat(column5.getAlias().get(), equalTo("KSQL_COL_5"));
    assertThat(column5.getExpression().toString(), equalTo("-5"));
  }

  @Test
  public void testBooleanLogicalExpression() {
    final String queryStr =
        "SELECT 10, col2, 'test', 2.5, true, -5 FROM test1 WHERE col1 = 10 AND col2 LIKE 'val' OR col4 > 2.6 ;";
    final Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    final StatementRewriter statementRewriter = new StatementRewriter();
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));

    final Query query = (Query) rewrittenStatement;
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    final QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    final SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias().get(), equalTo("KSQL_COL_0"));
    assertThat(column0.getExpression().toString(), equalTo("10"));

    final SingleColumn column1 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(1);
    assertThat(column1.getAlias().get(), equalTo("COL2"));
    assertThat(column1.getExpression().toString(), equalTo("TEST1.COL2"));

    final SingleColumn column2 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(2);
    assertThat(column2.getAlias().get(), equalTo("KSQL_COL_2"));
    assertThat(column2.getExpression().toString(), equalTo("'test'"));

  }

  @Test
  public void testSimpleLeftJoin() {
    final String queryStr =
        "SELECT t1.col1, t2.col1, t2.col4, col5, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON "
            + "t1.col1 = t2.col1;";
    final Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    final StatementRewriter statementRewriter = new StatementRewriter();
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));

    final Query query = (Query) rewrittenStatement;
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    final QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    assertThat(querySpecification.getFrom(), instanceOf(Join.class));
    final Join join = (Join) querySpecification.getFrom();
    assertThat(join.getType().toString(), equalTo("LEFT"));

    assertThat(((AliasedRelation)join.getLeft()).getAlias(), equalTo("T1"));
    assertThat(((AliasedRelation)join.getRight()).getAlias(), equalTo("T2"));

  }

  @Test
  public void testLeftJoinWithFilter() {
    final String queryStr =
        "SELECT t1.col1, t2.col1, t2.col4, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON t1.col1 = "
            + "t2.col1 WHERE t2.col2 = 'test';";
    final Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    final StatementRewriter statementRewriter = new StatementRewriter();
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));

    final Query query = (Query) rewrittenStatement;
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    final QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    assertThat(querySpecification.getFrom(), instanceOf(Join.class));
    final Join join = (Join) querySpecification.getFrom();
    assertThat(join.getType().toString(), equalTo("LEFT"));

    assertThat(((AliasedRelation)join.getLeft()).getAlias(), equalTo("T1"));
    assertThat(((AliasedRelation)join.getRight()).getAlias(), equalTo("T2"));

    assertThat(querySpecification.getWhere().get().toString(), equalTo("(T2.COL2 = 'test')"));
  }

  @Test
  public void testSelectAll() {
    final String queryStr = "SELECT * FROM test1 t1;";
    final Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    final StatementRewriter statementRewriter = new StatementRewriter();
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));

    final Query query = (Query) rewrittenStatement;
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    final QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    assertThat(querySpecification.getSelect().getSelectItems().size(), equalTo(8));
  }

  @Test
  public void testSelectAllJoin() {
    final String queryStr =
        "SELECT * FROM test1 t1 LEFT JOIN test2 t2 ON t1.col1 = t2.col1 WHERE t2.col2 = 'test';";
    final Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    final StatementRewriter statementRewriter = new StatementRewriter();
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));

    final Query query = (Query) rewrittenStatement;
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    final QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    assertThat("testSelectAllJoin fails", querySpecification.getFrom() instanceof Join);
    final Join join = (Join) querySpecification.getFrom();
    assertThat("testSelectAllJoin fails", querySpecification.getSelect().getSelectItems
        ().size() == 15);
    assertThat(((AliasedRelation)join.getLeft()).getAlias(), equalTo("T1"));
    assertThat(((AliasedRelation)join.getRight()).getAlias(), equalTo("T2"));
  }

  @Test
  public void testUDF() {
    final String queryStr = "SELECT lcase(col1), concat(col2,'hello'), floor(abs(col3)) FROM test1 t1;";
    final Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    final StatementRewriter statementRewriter = new StatementRewriter();
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));

    final Query query = (Query) rewrittenStatement;
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    final QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();

    final SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias().get(), equalTo("KSQL_COL_0"));
    assertThat(column0.getExpression().toString(), equalTo("LCASE(T1.COL1)"));

    final SingleColumn column1 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(1);
    assertThat(column1.getAlias().get(), equalTo("KSQL_COL_1"));
    assertThat(column1.getExpression().toString(), equalTo("CONCAT(T1.COL2, 'hello')"));

    final SingleColumn column2 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(2);
    assertThat(column2.getAlias().get(), equalTo("KSQL_COL_2"));
    assertThat(column2.getExpression().toString(), equalTo("FLOOR(ABS(T1.COL3))"));
  }

  @Test
  public void testCreateStreamWithTopic() {
    final String queryStr =
        "CREATE STREAM orders (ordertime bigint, orderid varchar, itemid varchar, orderunits "
            + "double) WITH (registered_topic = 'orders_topic' , key='ordertime');";
    final Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);

    final StatementRewriter statementRewriter = new StatementRewriter();
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(CreateStream.class));
    final CreateStream createStream = (CreateStream)rewrittenStatement;
    assertThat(createStream.getName().toString(), equalTo("ORDERS"));
    assertThat(createStream.getElements().size(), equalTo(4));
    assertThat(createStream.getElements().get(0).getName(), equalTo("ORDERTIME"));
    assertThat(createStream.getProperties().get(DdlConfig.TOPIC_NAME_PROPERTY).toString(), equalTo("'orders_topic'"));
  }

  @Test
  public void testCreateStreamWithTopicWithStruct() throws Exception {
    final String queryStr =
        "CREATE STREAM orders (ordertime bigint, orderid varchar, itemid varchar, orderunits "
            + "double, arraycol array<double>, mapcol map<varchar, double>, "
            + "order_address STRUCT < number VARCHAR, street VARCHAR, zip INTEGER, city "
            + "VARCHAR, state VARCHAR >) WITH (registered_topic = 'orders_topic' , key='ordertime');";
    final Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    final StatementRewriter statementRewriter = new StatementRewriter();
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(CreateStream.class));
    final CreateStream createStream = (CreateStream)rewrittenStatement;
    assertThat(createStream.getName().toString().toUpperCase(), equalTo("ORDERS"));
    assertThat(createStream.getElements().size(), equalTo(7));
    assertThat(createStream.getElements().get(0).getName().toLowerCase(), equalTo("ordertime"));
    assertThat(createStream.getElements().get(6).getType().getKsqlType(), equalTo(Type.KsqlType.STRUCT));
    final Struct struct = (Struct) createStream.getElements().get(6).getType();
    assertThat(struct.getItems().size(), equalTo(5));
    assertThat(struct.getItems().get(0).getRight().getKsqlType(), equalTo(Type.KsqlType.STRING));
    assertThat(createStream.getProperties().get(DdlConfig.TOPIC_NAME_PROPERTY).toString().toLowerCase(),
        equalTo("'orders_topic'"));
  }

  @Test
  public void testCreateStream() throws Exception {
    final String queryStr =
        "CREATE STREAM orders (ordertime bigint, orderid varchar, itemid varchar, orderunits "
            + "double) WITH (value_format = 'avro', "
            + "avroschemafile='/Users/hojjat/avro_order_schema.avro',kafka_topic='orders_topic');";
    final Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    final StatementRewriter statementRewriter = new StatementRewriter();
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(CreateStream.class));
    final CreateStream createStream = (CreateStream)rewrittenStatement;

    assertThat(createStream.getName().toString(), equalTo("ORDERS"));
    assertThat(createStream.getElements().size(), equalTo(4));
    assertThat(createStream.getElements().get(0).getName(), equalTo("ORDERTIME"));
    assertThat(createStream.getProperties().get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY).toString(), equalTo("'orders_topic'"));
    assertThat(createStream.getProperties().get(DdlConfig
        .VALUE_FORMAT_PROPERTY).toString(), equalTo("'avro'"));
    assertThat(createStream.getProperties().get(DdlConfig.AVRO_SCHEMA_FILE).toString(), equalTo("'/Users/hojjat/avro_order_schema.avro'"));
  }

  @Test
  public void testCreateTableWithTopic() {
    final String queryStr =
        "CREATE TABLE users (usertime bigint, userid varchar, regionid varchar, gender varchar) WITH (registered_topic = 'users_topic', key='userid', statestore='user_statestore');";
    final Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    final StatementRewriter statementRewriter = new StatementRewriter();
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);
    assertThat("testRegisterTopic failed.", rewrittenStatement instanceof CreateTable);
    final CreateTable createTable = (CreateTable)rewrittenStatement;
    assertThat(createTable.getName().toString(), equalTo("USERS"));
    assertThat(createTable.getElements().size(), equalTo(4));
    assertThat(createTable.getElements().get(0).getName(), equalTo("USERTIME"));
    assertThat(createTable.getProperties().get(DdlConfig.TOPIC_NAME_PROPERTY).toString(), equalTo("'users_topic'"));
  }

  @Test
  public void testCreateTable() {
    final String queryStr =
        "CREATE TABLE users (usertime bigint, userid varchar, regionid varchar, gender varchar) "
            + "WITH (kafka_topic = 'users_topic', value_format='json', key = 'userid');";
    final Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    final StatementRewriter statementRewriter = new StatementRewriter();
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);
    assertThat("testRegisterTopic failed.", rewrittenStatement instanceof CreateTable);
    final CreateTable createTable = (CreateTable)rewrittenStatement;
    assertThat(createTable.getName().toString(), equalTo("USERS"));
    assertThat(createTable.getElements().size(), equalTo(4));
    assertThat(createTable.getElements().get(0).getName(), equalTo("USERTIME"));
    assertThat(createTable.getProperties().get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY)
        .toString(), equalTo("'users_topic'"));
    assertThat(createTable.getProperties().get(DdlConfig.VALUE_FORMAT_PROPERTY)
        .toString(), equalTo("'json'"));
  }

  @Test
  public void testCreateStreamAsSelect() {

    final String queryStr =
        "CREATE STREAM bigorders_json WITH (value_format = 'json', "
            + "kafka_topic='bigorders_topic') AS SELECT * FROM orders WHERE orderunits > 5 ;";
    final Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);

    final StatementRewriter statementRewriter = new StatementRewriter();
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat("testCreateStreamAsSelect failed.", rewrittenStatement instanceof CreateStreamAsSelect);
    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect)rewrittenStatement;
    assertThat(createStreamAsSelect.getName().toString(), equalTo("BIGORDERS_JSON"));
    assertThat(createStreamAsSelect.getQuery().getQueryBody(), instanceOf(QuerySpecification.class));
    final QuerySpecification querySpecification = (QuerySpecification) createStreamAsSelect.getQuery().getQueryBody();
    assertThat(querySpecification.getSelect().getSelectItems().size(), equalTo(8));
    assertThat(querySpecification.getWhere().get().toString(), equalTo("(ORDERS.ORDERUNITS > 5)"));
    assertThat(((AliasedRelation)querySpecification.getFrom()).getAlias(), equalTo("ORDERS"));
  }


  @Test
  public void testSelectTumblingWindow() {

    final String queryStr =
        "select itemid, sum(orderunits) from orders window TUMBLING ( size 30 second) where orderunits > 5 group by itemid;";
    final Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);

    final StatementRewriter statementRewriter = new StatementRewriter();
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));
    final Query query = (Query) rewrittenStatement;
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    final QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
    assertThat(querySpecification.getSelect().getSelectItems().size(), equalTo(2));
    assertThat(querySpecification.getWhere().get().toString(), equalTo("(ORDERS.ORDERUNITS > 5)"));
    assertThat(((AliasedRelation)querySpecification.getFrom()).getAlias(), equalTo("ORDERS"));
    Assert.assertTrue( querySpecification.getWindowExpression().isPresent());
    assertThat(querySpecification
        .getWindowExpression().get().toString(), equalTo(" WINDOW STREAMWINDOW  TUMBLING ( SIZE 30 SECONDS ) "));
  }

  @Test
  public void testSelectHoppingWindow() {

    final String queryStr =
        "select itemid, sum(orderunits) from orders window HOPPING ( size 30 second, advance by 5"
            + " seconds) "
            + "where "
            + "orderunits"
            + " > 5 group by itemid;";
    final Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);

    final StatementRewriter statementRewriter = new StatementRewriter();
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));
    final Query query = (Query) rewrittenStatement;
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    final QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
    assertThat(querySpecification.getSelect().getSelectItems().size(), equalTo(2));
    assertThat(querySpecification.getWhere().get().toString(), equalTo("(ORDERS.ORDERUNITS > 5)"));
    assertThat(((AliasedRelation)querySpecification.getFrom()).getAlias().toUpperCase(), equalTo("ORDERS"));
    assertThat("window expression isn't present", querySpecification
        .getWindowExpression().isPresent());
    assertThat(querySpecification.getWindowExpression().get().toString().toUpperCase(),
        equalTo(" WINDOW STREAMWINDOW  HOPPING ( SIZE 30 SECONDS , ADVANCE BY 5 SECONDS ) "));
  }


  @Test
  public void testSelectSessionWindow() {

   final String queryStr =
        "select itemid, sum(orderunits) from orders window SESSION ( 30 second) where "
            + "orderunits > 5 group by itemid;";
    final Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);

    final StatementRewriter statementRewriter = new StatementRewriter();
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));
    final Query query = (Query) rewrittenStatement;
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    final QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
    assertThat(querySpecification.getSelect().getSelectItems().size(), equalTo(2));
    assertThat(querySpecification.getWhere().get().toString(), equalTo("(ORDERS.ORDERUNITS > 5)"));
    assertThat(((AliasedRelation)querySpecification.getFrom()).getAlias(), equalTo("ORDERS"));
    Assert.assertTrue( querySpecification.getWindowExpression().isPresent());
    assertThat(querySpecification
        .getWindowExpression().get().toString(), equalTo(" WINDOW STREAMWINDOW  SESSION "
        + "( 30 SECONDS ) "));
  }


  @Test
  public void testSelectSinkProperties() {
    final String simpleQuery = "create stream s1 with (timestamp='orderid', partitions = 3) as select "
        + "col1, col2"
        + " from orders where col2 is null and col3 is not null or (col3*col2 = "
        + "12);";
    final Statement statement = KSQL_PARSER.buildAst(simpleQuery, metaStore).get(0);

    final StatementRewriter statementRewriter = new StatementRewriter();
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(CreateStreamAsSelect.class));
    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) rewrittenStatement;
    assertThat(createStreamAsSelect.getQuery().getQueryBody()
        , instanceOf(QuerySpecification.class));
    final QuerySpecification querySpecification = (QuerySpecification)
        createStreamAsSelect.getQuery().getQueryBody();
    assertThat(querySpecification.getWhere().toString(), equalTo("Optional[(((ORDERS.COL2 IS NULL) AND (ORDERS.COL3 IS NOT NULL)) OR ((ORDERS.COL3 * ORDERS.COL2) = 12))]"));
  }

  @Test
  public void testInsertInto() {
    final String insertIntoString = "INSERT INTO test2 SELECT col0, col2, col3 FROM test1 WHERE col0 > "
        + "100;";
    final Statement statement = KSQL_PARSER.buildAst(insertIntoString, metaStore).get(0);

    final StatementRewriter statementRewriter = new StatementRewriter();
    final Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(InsertInto.class));
    final InsertInto insertInto = (InsertInto) rewrittenStatement;
    assertThat(insertInto.getTarget().toString(), equalTo("TEST2"));
    final Query query = insertInto.getQuery();
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    final QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    assertThat(querySpecification.getSelect().getSelectItems().size(), equalTo(3));
    assertThat(querySpecification.getFrom(), not(nullValue()));
    assertThat(querySpecification.getWhere().isPresent(), equalTo(true));
    assertThat(querySpecification.getWhere().get(),  instanceOf(ComparisonExpression.class));
    final ComparisonExpression comparisonExpression = (ComparisonExpression)querySpecification.getWhere().get();
    assertThat(comparisonExpression.getType().getValue(), equalTo(">"));

  }

}