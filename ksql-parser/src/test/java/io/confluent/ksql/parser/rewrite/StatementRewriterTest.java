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
  public void testSimpleQuery() {
    String simpleQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    Statement statement = KSQL_PARSER.buildAst(simpleQuery, metaStore).get(0);

    StatementRewriter statementRewriter = new StatementRewriter();
    Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat("testSimpleQuery fails", rewrittenStatement, instanceOf(Query.class));
    Query query = (Query) rewrittenStatement;
    assertThat("testSimpleQuery fails", query.getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    assertThat("testSimpleQuery fails", querySpecification.getSelect().getSelectItems().size() == 3);
    assertThat(querySpecification.getFrom(), not(nullValue()));
    assertThat("testSimpleQuery fails", querySpecification.getWhere().isPresent());
    assertThat("testSimpleQuery fails", querySpecification.getWhere().get() instanceof ComparisonExpression);
    ComparisonExpression comparisonExpression = (ComparisonExpression)querySpecification.getWhere().get();
    assertThat("testSimpleQuery fails", comparisonExpression.getType().getValue(), equalTo(">"));

  }

  @Test
  public void testProjection() {
    String queryStr = "SELECT col0, col2, col3 FROM test1;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);

    StatementRewriter statementRewriter = new StatementRewriter();
    Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat("testSimpleQuery fails", rewrittenStatement, instanceOf(Query.class));

    Query query = (Query) rewrittenStatement;
    assertThat("testProjection fails", query.getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    assertThat("testProjection fails", querySpecification.getSelect().getSelectItems().size() , equalTo(3));
    assertThat("testProjection fails", querySpecification.getSelect().getSelectItems().get(0), instanceOf(SingleColumn.class));
    SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
    assertThat("testProjection fails", column0.getAlias().get(), equalTo("COL0"));
    assertThat("testProjection fails", column0.getExpression().toString(), equalTo("TEST1.COL0"));
  }

  @Test
  public void testProjectionWithArrayMap() {
    String queryStr = "SELECT col0, col2, col3, col4[0], col5['key1'] FROM test1;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);

    StatementRewriter statementRewriter = new StatementRewriter();
    Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat("testSimpleQuery fails", rewrittenStatement, instanceOf(Query.class));

    Query query = (Query) rewrittenStatement;
    assertThat("testProjectionWithArrayMap fails", query.getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    assertThat("testProjectionWithArrayMap fails", querySpecification.getSelect().getSelectItems()
        .size(), equalTo(5));
    assertThat("testProjectionWithArrayMap fails", querySpecification.getSelect().getSelectItems().get(0) instanceof SingleColumn);
    SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
    assertThat("testProjectionWithArrayMap fails", column0.getAlias().get(), equalTo("COL0"));
    assertThat("testProjectionWithArrayMap fails", column0.getExpression().toString(), equalTo("TEST1.COL0"));

    SingleColumn column3 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(3);
    SingleColumn column4 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(4);
    assertThat("testProjectionWithArrayMap fails", column3.getExpression().toString(), equalTo("TEST1.COL4[0]"));
    assertThat("testProjectionWithArrayMap fails", column4.getExpression().toString(), equalTo("TEST1.COL5['key1']"));
  }

  @Test
  public void testProjectFilter() {
    String queryStr = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    StatementRewriter statementRewriter = new StatementRewriter();
    Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat("testSimpleQuery fails", rewrittenStatement, instanceOf(Query.class));

    Query query = (Query) rewrittenStatement;
    assertThat("testProjectFilter fails", query.getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();

    assertThat("testProjectFilter fails", querySpecification.getWhere().get(), instanceOf(ComparisonExpression.class));
    ComparisonExpression comparisonExpression = (ComparisonExpression)querySpecification.getWhere().get();
    assertThat("testProjectFilter fails", comparisonExpression.toString(), equalTo("(TEST1.COL0 > 100)"));
    assertThat("testProjectFilter fails", querySpecification.getSelect().getSelectItems().size(), equalTo(3));

  }

  @Test
  public void testBinaryExpression() {
    String queryStr = "SELECT col0+10, col2, col3-col1 FROM test1;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    StatementRewriter statementRewriter = new StatementRewriter();
    Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat("testSimpleQuery fails", rewrittenStatement, instanceOf(Query.class));

    Query query = (Query) rewrittenStatement;
    assertThat("testBinaryExpression fails", query.getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
    assertThat("testBinaryExpression fails", column0.getAlias().get(), equalTo("KSQL_COL_0"));
    assertThat("testBinaryExpression fails", column0.getExpression().toString(), equalTo("(TEST1.COL0 + 10)"));
  }

  @Test
  public void testBooleanExpression() {
    String queryStr = "SELECT col0 = 10, col2, col3 > col1 FROM test1;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    StatementRewriter statementRewriter = new StatementRewriter();
    Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat("testSimpleQuery fails", rewrittenStatement, instanceOf(Query.class));

    Query query = (Query) rewrittenStatement;
    assertThat("testProjection fails", query.getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
    assertThat("testBooleanExpression fails", column0.getAlias().get(), equalTo("KSQL_COL_0"));
    assertThat("testBooleanExpression fails", column0.getExpression().toString(), equalTo("(TEST1.COL0 = 10)"));
  }

  @Test
  public void testLiterals() {
    String queryStr = "SELECT 10, col2, 'test', 2.5, true, -5 FROM test1;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    StatementRewriter statementRewriter = new StatementRewriter();
    Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat("testSimpleQuery fails", rewrittenStatement, instanceOf(Query.class));

    Query query = (Query) rewrittenStatement;
    assertThat("testLiterals fails", query.getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
    assertThat("testLiterals fails", column0.getAlias().get(), equalTo("KSQL_COL_0"));
    assertThat("testLiterals fails", column0.getExpression().toString(), equalTo("10"));

    SingleColumn column1 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(1);
    assertThat("testLiterals fails", column1.getAlias().get(), equalTo("COL2"));
    assertThat("testLiterals fails", column1.getExpression().toString(), equalTo("TEST1.COL2"));

    SingleColumn column2 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(2);
    assertThat("testLiterals fails", column2.getAlias().get(), equalTo("KSQL_COL_2"));
    assertThat("testLiterals fails", column2.getExpression().toString(), equalTo("'test'"));

    SingleColumn column3 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(3);
    assertThat("testLiterals fails", column3.getAlias().get(), equalTo("KSQL_COL_3"));
    assertThat("testLiterals fails", column3.getExpression().toString(), equalTo("2.5"));

    SingleColumn column4 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(4);
    assertThat("testLiterals fails", column4.getAlias().get(), equalTo("KSQL_COL_4"));
    assertThat("testLiterals fails", column4.getExpression().toString(), equalTo("true"));

    SingleColumn column5 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(5);
    assertThat("testLiterals fails", column5.getAlias().get(), equalTo("KSQL_COL_5"));
    assertThat("testLiterals fails", column5.getExpression().toString(), equalTo("-5"));
  }

  @Test
  public void testBooleanLogicalExpression() {
    String
        queryStr =
        "SELECT 10, col2, 'test', 2.5, true, -5 FROM test1 WHERE col1 = 10 AND col2 LIKE 'val' OR col4 > 2.6 ;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    StatementRewriter statementRewriter = new StatementRewriter();
    Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat("testSimpleQuery fails", rewrittenStatement, instanceOf(Query.class));

    Query query = (Query) rewrittenStatement;
    assertThat("testProjection fails", query.getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
    assertThat("testProjection fails", column0.getAlias().get(), equalTo("KSQL_COL_0"));
    assertThat("testProjection fails", column0.getExpression().toString(), equalTo("10"));

    SingleColumn column1 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(1);
    assertThat("testProjection fails", column1.getAlias().get(), equalTo("COL2"));
    assertThat("testProjection fails", column1.getExpression().toString(), equalTo("TEST1.COL2"));

    SingleColumn column2 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(2);
    assertThat("testProjection fails", column2.getAlias().get(), equalTo("KSQL_COL_2"));
    assertThat("testProjection fails", column2.getExpression().toString(), equalTo("'test'"));

  }

  @Test
  public void testSimpleLeftJoin() {
    String
        queryStr =
        "SELECT t1.col1, t2.col1, t2.col4, col5, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON "
            + "t1.col1 = t2.col1;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    StatementRewriter statementRewriter = new StatementRewriter();
    Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat("testSimpleQuery fails", rewrittenStatement, instanceOf(Query.class));

    Query query = (Query) rewrittenStatement;
    assertThat("testSimpleLeftJoin fails", query.getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    assertThat("testSimpleLeftJoin fails", querySpecification.getFrom() instanceof Join);
    Join join = (Join) querySpecification.getFrom();
    assertThat("testSimpleLeftJoin fails", join.getType().toString(), equalTo("LEFT"));

    assertThat("testSimpleLeftJoin fails", ((AliasedRelation)join.getLeft()).getAlias(), equalTo("T1"));
    assertThat("testSimpleLeftJoin fails", ((AliasedRelation)join.getRight()).getAlias(), equalTo("T2"));

  }

  @Test
  public void testLeftJoinWithFilter() {
    String
        queryStr =
        "SELECT t1.col1, t2.col1, t2.col4, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON t1.col1 = "
            + "t2.col1 WHERE t2.col2 = 'test';";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    StatementRewriter statementRewriter = new StatementRewriter();
    Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat("testSimpleQuery fails", rewrittenStatement, instanceOf(Query.class));

    Query query = (Query) rewrittenStatement;
    assertThat("testLeftJoinWithFilter fails", query.getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    assertThat("testLeftJoinWithFilter fails", querySpecification.getFrom() instanceof Join);
    Join join = (Join) querySpecification.getFrom();
    assertThat("testLeftJoinWithFilter fails", join.getType().toString(), equalTo("LEFT"));

    assertThat("testLeftJoinWithFilter fails", ((AliasedRelation)join.getLeft()).getAlias(), equalTo("T1"));
    assertThat("testLeftJoinWithFilter fails", ((AliasedRelation)join.getRight()).getAlias(), equalTo("T2"));

    assertThat("testLeftJoinWithFilter fails", querySpecification.getWhere().get().toString(), equalTo("(T2.COL2 = 'test')"));
  }

  @Test
  public void testSelectAll() {
    String queryStr = "SELECT * FROM test1 t1;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    StatementRewriter statementRewriter = new StatementRewriter();
    Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat("testSimpleQuery fails", rewrittenStatement, instanceOf(Query.class));

    Query query = (Query) rewrittenStatement;
    assertThat("testSelectAll fails", query.getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    assertThat("testSelectAll fails", querySpecification.getSelect().getSelectItems()
        .size() == 8);
  }

  @Test
  public void testSelectAllJoin() {
    String
        queryStr =
        "SELECT * FROM test1 t1 LEFT JOIN test2 t2 ON t1.col1 = t2.col1 WHERE t2.col2 = 'test';";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    StatementRewriter statementRewriter = new StatementRewriter();
    Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat("testSimpleQuery fails", rewrittenStatement, instanceOf(Query.class));

    Query query = (Query) rewrittenStatement;
    assertThat("testLeftJoinWithFilter fails", query.getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    assertThat("testSelectAllJoin fails", querySpecification.getFrom() instanceof Join);
    Join join = (Join) querySpecification.getFrom();
    assertThat("testSelectAllJoin fails", querySpecification.getSelect().getSelectItems
        ().size() == 15);
    assertThat("testLeftJoinWithFilter fails", ((AliasedRelation)join.getLeft()).getAlias(), equalTo("T1"));
    assertThat("testLeftJoinWithFilter fails", ((AliasedRelation)join.getRight()).getAlias(), equalTo("T2"));
  }

  @Test
  public void testUDF() {
    String queryStr = "SELECT lcase(col1), concat(col2,'hello'), floor(abs(col3)) FROM test1 t1;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    StatementRewriter statementRewriter = new StatementRewriter();
    Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat("testSimpleQuery fails", rewrittenStatement, instanceOf(Query.class));

    Query query = (Query) rewrittenStatement;
    assertThat("testSelectAll fails", query.getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();

    SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
    assertThat("testProjection fails", column0.getAlias().get(), equalTo("KSQL_COL_0"));
    assertThat("testProjection fails", column0.getExpression().toString(), equalTo("LCASE(T1.COL1)"));

    SingleColumn column1 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(1);
    assertThat("testProjection fails", column1.getAlias().get(), equalTo("KSQL_COL_1"));
    assertThat("testProjection fails", column1.getExpression().toString(), equalTo("CONCAT(T1.COL2, 'hello')"));

    SingleColumn column2 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(2);
    assertThat("testProjection fails", column2.getAlias().get(), equalTo("KSQL_COL_2"));
    assertThat("testProjection fails", column2.getExpression().toString(), equalTo("FLOOR(ABS(T1.COL3))"));
  }

  @Test
  public void testCreateStreamWithTopic() {
    String
        queryStr =
        "CREATE STREAM orders (ordertime bigint, orderid varchar, itemid varchar, orderunits "
            + "double) WITH (registered_topic = 'orders_topic' , key='ordertime');";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);

    StatementRewriter statementRewriter = new StatementRewriter();
    Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat("testCreateStream failed.", rewrittenStatement instanceof CreateStream);
    CreateStream createStream = (CreateStream)rewrittenStatement;
    assertThat("testCreateStream failed.", createStream.getName().toString(), equalTo("ORDERS"));
    assertThat("testCreateStream failed.", createStream.getElements().size() == 4);
    assertThat("testCreateStream failed.", createStream.getElements().get(0).getName(), equalTo("ORDERTIME"));
    assertThat("testCreateStream failed.", createStream.getProperties().get(DdlConfig.TOPIC_NAME_PROPERTY).toString(), equalTo("'orders_topic'"));
  }

  @Test
  public void testCreateStreamWithTopicWithStruct() throws Exception {
    String
        queryStr =
        "CREATE STREAM orders (ordertime bigint, orderid varchar, itemid varchar, orderunits "
            + "double, arraycol array<double>, mapcol map<varchar, double>, "
            + "order_address STRUCT < number VARCHAR, street VARCHAR, zip INTEGER, city "
            + "VARCHAR, state VARCHAR >) WITH (registered_topic = 'orders_topic' , key='ordertime');";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    StatementRewriter statementRewriter = new StatementRewriter();
    Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat("testCreateStream failed.", rewrittenStatement instanceof CreateStream);
    CreateStream createStream = (CreateStream)rewrittenStatement;
    assertThat(createStream.getName().toString().toUpperCase(), equalTo("ORDERS"));
    assertThat(createStream.getElements().size(), equalTo(7));
    assertThat(createStream.getElements().get(0).getName().toString().toLowerCase(), equalTo("ordertime"));
    assertThat(createStream.getElements().get(6).getType().getKsqlType(), equalTo(Type.KsqlType.STRUCT));
    Struct struct = (Struct) createStream.getElements().get(6).getType();
    assertThat(struct.getItems().size(), equalTo(5));
    assertThat(struct.getItems().get(0).getRight().getKsqlType(), equalTo(Type.KsqlType.STRING));
    assertThat(createStream.getProperties().get(DdlConfig.TOPIC_NAME_PROPERTY).toString().toLowerCase(),
        equalTo("'orders_topic'"));
  }

  @Test
  public void testCreateStream() throws Exception {
    String
        queryStr =
        "CREATE STREAM orders (ordertime bigint, orderid varchar, itemid varchar, orderunits "
            + "double) WITH (value_format = 'avro', "
            + "avroschemafile='/Users/hojjat/avro_order_schema.avro',kafka_topic='orders_topic');";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    StatementRewriter statementRewriter = new StatementRewriter();
    Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat("testCreateStream failed.", rewrittenStatement instanceof CreateStream);
    CreateStream createStream = (CreateStream)rewrittenStatement;

    assertThat("testCreateStream failed.", createStream.getName().toString(), equalTo("ORDERS"));
    assertThat("testCreateStream failed.", createStream.getElements().size() == 4);
    assertThat("testCreateStream failed.", createStream.getElements().get(0).getName(), equalTo("ORDERTIME"));
    assertThat("testCreateStream failed.", createStream.getProperties().get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY).toString(), equalTo("'orders_topic'"));
    assertThat("testCreateStream failed.", createStream.getProperties().get(DdlConfig
        .VALUE_FORMAT_PROPERTY).toString(), equalTo("'avro'"));
    assertThat("testCreateStream failed.", createStream.getProperties().get(DdlConfig.AVRO_SCHEMA_FILE).toString(), equalTo("'/Users/hojjat/avro_order_schema.avro'"));
  }

  @Test
  public void testCreateTableWithTopic() {
    String
        queryStr =
        "CREATE TABLE users (usertime bigint, userid varchar, regionid varchar, gender varchar) WITH (registered_topic = 'users_topic', key='userid', statestore='user_statestore');";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    StatementRewriter statementRewriter = new StatementRewriter();
    Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);
    assertThat("testRegisterTopic failed.", rewrittenStatement instanceof CreateTable);
    CreateTable createTable = (CreateTable)rewrittenStatement;
    assertThat("testCreateTable failed.", createTable.getName().toString(), equalTo("USERS"));
    assertThat("testCreateTable failed.", createTable.getElements().size() == 4);
    assertThat("testCreateTable failed.", createTable.getElements().get(0).getName(), equalTo("USERTIME"));
    assertThat("testCreateTable failed.", createTable.getProperties().get(DdlConfig.TOPIC_NAME_PROPERTY).toString(), equalTo("'users_topic'"));
  }

  @Test
  public void testCreateTable() {
    String
        queryStr =
        "CREATE TABLE users (usertime bigint, userid varchar, regionid varchar, gender varchar) "
            + "WITH (kafka_topic = 'users_topic', value_format='json', key = 'userid');";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    StatementRewriter statementRewriter = new StatementRewriter();
    Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);
    assertThat("testRegisterTopic failed.", rewrittenStatement instanceof CreateTable);
    CreateTable createTable = (CreateTable)rewrittenStatement;
    assertThat("testCreateTable failed.", createTable.getName().toString(), equalTo("USERS"));
    assertThat("testCreateTable failed.", createTable.getElements().size() == 4);
    assertThat("testCreateTable failed.", createTable.getElements().get(0).getName(), equalTo("USERTIME"));
    assertThat("testCreateTable failed.", createTable.getProperties().get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY)
        .toString(), equalTo("'users_topic'"));
    assertThat("testCreateTable failed.", createTable.getProperties().get(DdlConfig.VALUE_FORMAT_PROPERTY)
        .toString(), equalTo("'json'"));
  }

  @Test
  public void testCreateStreamAsSelect() {

    String
        queryStr =
        "CREATE STREAM bigorders_json WITH (value_format = 'json', "
            + "kafka_topic='bigorders_topic') AS SELECT * FROM orders WHERE orderunits > 5 ;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);

    StatementRewriter statementRewriter = new StatementRewriter();
    Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat("testCreateStreamAsSelect failed.", rewrittenStatement instanceof CreateStreamAsSelect);
    CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect)rewrittenStatement;
    assertThat("testCreateTable failed.", createStreamAsSelect.getName().toString(), equalTo("BIGORDERS_JSON"));
    assertThat("testCreateTable failed.", createStreamAsSelect.getQuery().getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification) createStreamAsSelect.getQuery().getQueryBody();
    assertThat("testCreateTable failed.", querySpecification.getSelect().getSelectItems().size() == 4);
    assertThat("testCreateTable failed.", querySpecification.getWhere().get().toString(), equalTo("(ORDERS.ORDERUNITS > 5)"));
    assertThat("testCreateTable failed.", ((AliasedRelation)querySpecification.getFrom()).getAlias(), equalTo("ORDERS"));
  }


  @Test
  public void testSelectTumblingWindow() {

    String
        queryStr =
        "select itemid, sum(orderunits) from orders window TUMBLING ( size 30 second) where orderunits > 5 group by itemid;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);

    StatementRewriter statementRewriter = new StatementRewriter();
    Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat("testSelectTumblingWindow failed.", rewrittenStatement, instanceOf(Query.class));
    Query query = (Query) rewrittenStatement;
    assertThat("testSelectTumblingWindow failed.", query.getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
    assertThat("testCreateTable failed.", querySpecification.getSelect().getSelectItems
        ().size() == 2);
    assertThat("testSelectTumblingWindow failed.", querySpecification.getWhere().get().toString(), equalTo("(ORDERS.ORDERUNITS > 5)"));
    assertThat("testSelectTumblingWindow failed.", ((AliasedRelation)querySpecification.getFrom()).getAlias(), equalTo("ORDERS"));
    assertThat("testSelectTumblingWindow failed.", querySpecification
        .getWindowExpression().isPresent());
    assertThat("testSelectTumblingWindow failed.", querySpecification
        .getWindowExpression().get().toString(), equalTo(" WINDOW STREAMWINDOW  TUMBLING ( SIZE 30 SECONDS ) "));
  }

  @Test
  public void testSelectHoppingWindow() {

    String
        queryStr =
        "select itemid, sum(orderunits) from orders window HOPPING ( size 30 second, advance by 5"
            + " seconds) "
            + "where "
            + "orderunits"
            + " > 5 group by itemid;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);

    StatementRewriter statementRewriter = new StatementRewriter();
    Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(Query.class));
    Query query = (Query) rewrittenStatement;
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
    assertThat(querySpecification.getSelect().getSelectItems().size(), equalTo(2));
    assertThat(querySpecification.getWhere().get().toString(), equalTo("(ORDERS.ORDERUNITS > 5)"));
    assertThat(((AliasedRelation)querySpecification.getFrom()).getAlias().toUpperCase(), equalTo("ORDERS"));
    assertThat("window expression isn't present", querySpecification
        .getWindowExpression().isPresent());
    assertThat(querySpecification.getWindowExpression().get().toString().toUpperCase(),
        equalTo(" WINDOW STREAMWINDOW  HOPPING ( SIZE 30 SECONDS , ADVANCE BY 5 SECONDS ) "));
  }

  @Test
  public void should() {
    List<Statement> statements = KSQL_PARSER.buildAst("select * from orders;", metaStore);
    System.out.println(statements);
  }
  @Test
  public void testSelectSessionWindow() {

    String
        queryStr =
        "select itemid, sum(orderunits) from orders window SESSION ( 30 second) where "
            + "orderunits > 5 group by itemid;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);

    StatementRewriter statementRewriter = new StatementRewriter();
    Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat("testSelectSessionWindow failed.", rewrittenStatement, instanceOf(Query.class));
    Query query = (Query) rewrittenStatement;
    assertThat("testSelectSessionWindow failed.", query.getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
    assertThat("testCreateTable failed.", querySpecification.getSelect().getSelectItems
        ().size() == 2);
    assertThat("testSelectSessionWindow failed.", querySpecification.getWhere().get().toString(), equalTo("(ORDERS.ORDERUNITS > 5)"));
    assertThat("testSelectSessionWindow failed.", ((AliasedRelation)querySpecification.getFrom()).getAlias(), equalTo("ORDERS"));
    assertThat("testSelectSessionWindow failed.", querySpecification
        .getWindowExpression().isPresent());
    assertThat("testSelectSessionWindow failed.", querySpecification
        .getWindowExpression().get().toString(), equalTo(" WINDOW STREAMWINDOW  SESSION "
        + "( 30 SECONDS ) "));
  }


  @Test
  public void testSelectSinkProperties() {
    String simpleQuery = "create stream s1 with (timestamp='orderid', partitions = 3) as select "
        + "col1, col2"
        + " from orders where col2 is null and col3 is not null or (col3*col2 = "
        + "12);";
    Statement statement = KSQL_PARSER.buildAst(simpleQuery, metaStore).get(0);

    StatementRewriter statementRewriter = new StatementRewriter();
    Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat("testSelectTumblingWindow failed.", rewrittenStatement instanceof CreateStreamAsSelect);
    CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) rewrittenStatement;
    assertThat("testSelectTumblingWindow failed.", createStreamAsSelect.getQuery().getQueryBody()
        , instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification)
        createStreamAsSelect.getQuery().getQueryBody();
    assertThat(querySpecification.getWhere().toString(), equalTo("Optional[(((ORDERS.COL2 IS NULL) AND (ORDERS.COL3 IS NOT NULL)) OR ((ORDERS.COL3 * ORDERS.COL2) = 12))]"));
  }

  @Test
  public void testInsertInto() {
    String insertIntoString = "INSERT INTO test2 SELECT col0, col2, col3 FROM test1 WHERE col0 > "
        + "100;";
    Statement statement = KSQL_PARSER.buildAst(insertIntoString, metaStore).get(0);

    StatementRewriter statementRewriter = new StatementRewriter();
    Statement rewrittenStatement = (Statement) statementRewriter.process(statement, null);

    assertThat(rewrittenStatement, instanceOf(InsertInto.class));
    InsertInto insertInto = (InsertInto) rewrittenStatement;
    assertThat(insertInto.getTarget().toString(), equalTo("TEST2"));
    Query query = insertInto.getQuery();
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    assertThat( querySpecification.getSelect().getSelectItems().size(), equalTo(3));
    assertThat(querySpecification.getFrom(), not(nullValue()));
    assertThat(querySpecification.getWhere().isPresent(), equalTo(true));
    assertThat(querySpecification.getWhere().get(),  instanceOf(ComparisonExpression.class));
    ComparisonExpression comparisonExpression = (ComparisonExpression)querySpecification.getWhere().get();
    assertThat(comparisonExpression.getType().getValue(), equalTo(">"));

  }

}