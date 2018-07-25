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

package io.confluent.ksql.parser;

import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.IntegerLiteral;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.parser.tree.ListQueries;
import io.confluent.ksql.parser.tree.ListStreams;
import io.confluent.ksql.parser.tree.ListTables;
import io.confluent.ksql.parser.tree.ListTopics;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.parser.tree.LongLiteral;
import io.confluent.ksql.parser.tree.RegisterTopic;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.QuerySpecification;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Struct;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;


public class KsqlParserTest {

  private static final KsqlParser KSQL_PARSER = new KsqlParser();

  private MetaStore metaStore;

  @Before
  public void init() {

    metaStore = MetaStoreFixture.getNewMetaStore(new TestFunctionRegistry());

    final Schema addressSchema = SchemaBuilder.struct()
        .field("NUMBER", Schema.OPTIONAL_INT64_SCHEMA)
        .field("STREET", Schema.OPTIONAL_STRING_SCHEMA)
        .field("CITY", Schema.OPTIONAL_STRING_SCHEMA)
        .field("STATE", Schema.OPTIONAL_STRING_SCHEMA)
        .field("ZIPCODE", Schema.OPTIONAL_INT64_SCHEMA)
        .optional().build();

    final Schema categorySchema = SchemaBuilder.struct()
        .field("ID", Schema.OPTIONAL_INT64_SCHEMA)
        .field("NAME", Schema.OPTIONAL_STRING_SCHEMA)
        .optional().build();

    final Schema itemInfoSchema = SchemaBuilder.struct()
        .field("ITEMID", Schema.INT64_SCHEMA)
        .field("NAME", Schema.STRING_SCHEMA)
        .field("CATEGORY", categorySchema)
        .optional().build();

    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    final Schema schemaBuilderOrders = schemaBuilder
        .field("ORDERTIME", Schema.INT64_SCHEMA)
        .field("ORDERID", Schema.OPTIONAL_INT64_SCHEMA)
        .field("ITEMID", Schema.OPTIONAL_STRING_SCHEMA)
        .field("ITEMINFO", itemInfoSchema)
        .field("ORDERUNITS", Schema.INT32_SCHEMA)
        .field("ARRAYCOL",SchemaBuilder.array(Schema.FLOAT64_SCHEMA).optional().build())
        .field("MAPCOL", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.FLOAT64_SCHEMA).optional().build())
        .field("ADDRESS", addressSchema)
        .build();

    KsqlTopic
        ksqlTopicOrders =
        new KsqlTopic("ADDRESS_TOPIC", "orders_topic", new KsqlJsonTopicSerDe());

    KsqlStream ksqlStreamOrders = new KsqlStream(
        "sqlexpression",
        "ADDRESS",
        schemaBuilderOrders,
        schemaBuilderOrders.field("ORDERTIME"),
        new MetadataTimestampExtractionPolicy(),
        ksqlTopicOrders);

    metaStore.putTopic(ksqlTopicOrders);
    metaStore.putSource(ksqlStreamOrders);

    KsqlTopic
        ksqlTopicItems =
        new KsqlTopic("ITEMS_TOPIC", "item_topic", new KsqlJsonTopicSerDe());
    KsqlTable ksqlTableOrders = new KsqlTable(
        "sqlexpression",
        "ITEMID",
        itemInfoSchema,
        itemInfoSchema.field("ITEMID"),
        new MetadataTimestampExtractionPolicy(),
        ksqlTopicItems,
        "items",
        false);
    metaStore.putTopic(ksqlTopicItems);
    metaStore.putSource(ksqlTableOrders);
  }

  @Test
  public void testSimpleQuery() {
    String simpleQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    Statement statement = KSQL_PARSER.buildAst(simpleQuery, metaStore).get(0);


    Assert.assertTrue("testSimpleQuery fails", statement instanceof Query);
    Query query = (Query) statement;
    Assert.assertTrue("testSimpleQuery fails", query.getQueryBody() instanceof QuerySpecification);
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    Assert.assertTrue("testSimpleQuery fails", querySpecification.getSelect().getSelectItems().size() == 3);
    assertThat(querySpecification.getFrom(), not(nullValue()));
    Assert.assertTrue("testSimpleQuery fails", querySpecification.getWhere().isPresent());
    Assert.assertTrue("testSimpleQuery fails", querySpecification.getWhere().get() instanceof ComparisonExpression);
    ComparisonExpression comparisonExpression = (ComparisonExpression)querySpecification.getWhere().get();
    Assert.assertTrue("testSimpleQuery fails", comparisonExpression.getType().getValue().equalsIgnoreCase(">"));

  }

  @Test
  public void testProjection() {
    String queryStr = "SELECT col0, col2, col3 FROM test1;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    Assert.assertTrue("testProjection fails", statement instanceof Query);
    Query query = (Query) statement;
    Assert.assertTrue("testProjection fails", query.getQueryBody() instanceof QuerySpecification);
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    Assert.assertTrue("testProjection fails", querySpecification.getSelect().getSelectItems().size() == 3);
    Assert.assertTrue("testProjection fails", querySpecification.getSelect().getSelectItems().get(0) instanceof SingleColumn);
    SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
    Assert.assertTrue("testProjection fails", column0.getAlias().get().equalsIgnoreCase("COL0"));
    Assert.assertTrue("testProjection fails", column0.getExpression().toString().equalsIgnoreCase("TEST1.COL0"));
  }

  @Test
  public void testProjectionWithArrayMap() {
    String queryStr = "SELECT col0, col2, col3, col4[0], col5['key1'] FROM test1;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    Assert.assertTrue("testProjectionWithArrayMap fails", statement instanceof Query);
    Query query = (Query) statement;
    Assert.assertTrue("testProjectionWithArrayMap fails", query.getQueryBody() instanceof QuerySpecification);
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    Assert.assertTrue("testProjectionWithArrayMap fails", querySpecification.getSelect().getSelectItems()
                                                  .size() == 5);
    Assert.assertTrue("testProjectionWithArrayMap fails", querySpecification.getSelect().getSelectItems().get(0) instanceof SingleColumn);
    SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
    Assert.assertTrue("testProjectionWithArrayMap fails", column0.getAlias().get().equalsIgnoreCase("COL0"));
    Assert.assertTrue("testProjectionWithArrayMap fails", column0.getExpression().toString().equalsIgnoreCase("TEST1.COL0"));

    SingleColumn column3 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(3);
    SingleColumn column4 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(4);
    Assert.assertTrue("testProjectionWithArrayMap fails", column3.getExpression().toString()
        .equalsIgnoreCase("TEST1.COL4[0]"));
    Assert.assertTrue("testProjectionWithArrayMap fails", column4.getExpression().toString()
        .equalsIgnoreCase("TEST1.COL5['key1']"));
  }

  @Test
  public void testProjectFilter() {
    String queryStr = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    Assert.assertTrue("testSimpleQuery fails", statement instanceof Query);
    Query query = (Query) statement;
    Assert.assertTrue("testProjectFilter fails", query.getQueryBody() instanceof QuerySpecification);
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();

    Assert.assertTrue("testProjectFilter fails", querySpecification.getWhere().get() instanceof ComparisonExpression);
    ComparisonExpression comparisonExpression = (ComparisonExpression)querySpecification.getWhere().get();
    Assert.assertTrue("testProjectFilter fails", comparisonExpression.toString().equalsIgnoreCase("(TEST1.COL0 > 100)"));
    Assert.assertTrue("testProjectFilter fails", querySpecification.getSelect().getSelectItems().size() == 3);

  }

  @Test
  public void testBinaryExpression() {
    String queryStr = "SELECT col0+10, col2, col3-col1 FROM test1;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    Assert.assertTrue("testBinaryExpression fails", statement instanceof Query);
    Query query = (Query) statement;
    Assert.assertTrue("testBinaryExpression fails", query.getQueryBody() instanceof QuerySpecification);
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
    Assert.assertTrue("testBinaryExpression fails", column0.getAlias().get().equalsIgnoreCase("KSQL_COL_0"));
    Assert.assertTrue("testBinaryExpression fails", column0.getExpression().toString().equalsIgnoreCase("(TEST1.COL0 + 10)"));
  }

  @Test
  public void testBooleanExpression() {
    String queryStr = "SELECT col0 = 10, col2, col3 > col1 FROM test1;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    Assert.assertTrue("testBooleanExpression fails", statement instanceof Query);
    Query query = (Query) statement;
    Assert.assertTrue("testProjection fails", query.getQueryBody() instanceof QuerySpecification);
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
    Assert.assertTrue("testBooleanExpression fails", column0.getAlias().get().equalsIgnoreCase("KSQL_COL_0"));
    Assert.assertTrue("testBooleanExpression fails", column0.getExpression().toString().equalsIgnoreCase("(TEST1.COL0 = 10)"));
  }

  @Test
  public void testLiterals() {
    String queryStr = "SELECT 10, col2, 'test', 2.5, true, -5 FROM test1;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    Assert.assertTrue("testLiterals fails", statement instanceof Query);
    Query query = (Query) statement;
    Assert.assertTrue("testLiterals fails", query.getQueryBody() instanceof QuerySpecification);
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
    Assert.assertTrue("testLiterals fails", column0.getAlias().get().equalsIgnoreCase("KSQL_COL_0"));
    Assert.assertTrue("testLiterals fails", column0.getExpression().toString().equalsIgnoreCase("10"));

    SingleColumn column1 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(1);
    Assert.assertTrue("testLiterals fails", column1.getAlias().get().equalsIgnoreCase("COL2"));
    Assert.assertTrue("testLiterals fails", column1.getExpression().toString().equalsIgnoreCase("TEST1.COL2"));

    SingleColumn column2 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(2);
    Assert.assertTrue("testLiterals fails", column2.getAlias().get().equalsIgnoreCase("KSQL_COL_2"));
    Assert.assertTrue("testLiterals fails", column2.getExpression().toString().equalsIgnoreCase("'test'"));

    SingleColumn column3 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(3);
    Assert.assertTrue("testLiterals fails", column3.getAlias().get().equalsIgnoreCase("KSQL_COL_3"));
    Assert.assertTrue("testLiterals fails", column3.getExpression().toString().equalsIgnoreCase("2.5"));

    SingleColumn column4 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(4);
    Assert.assertTrue("testLiterals fails", column4.getAlias().get().equalsIgnoreCase("KSQL_COL_4"));
    Assert.assertTrue("testLiterals fails", column4.getExpression().toString().equalsIgnoreCase("true"));

    SingleColumn column5 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(5);
    Assert.assertTrue("testLiterals fails", column5.getAlias().get().equalsIgnoreCase("KSQL_COL_5"));
    Assert.assertTrue("testLiterals fails", column5.getExpression().toString().equalsIgnoreCase("-5"));
  }

  private <T, L extends Literal> void shouldParseNumericLiteral(final T value,
                                                                final L expectedValue) {
    final String queryStr = String.format("SELECT " + value.toString() + " FROM test1;", value);
    final Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    assertThat(statement, instanceOf(Query.class));
    final Query query = (Query) statement;
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    final QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
    final SingleColumn column0
        = (SingleColumn) querySpecification.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias().get(), equalTo("KSQL_COL_0"));
    assertThat(column0.getExpression(), instanceOf(expectedValue.getClass()));
    assertThat(column0.getExpression(), equalTo(expectedValue));
  }

  @Test
  public void shouldParseIntegerLiterals() {
    shouldParseNumericLiteral(0, new IntegerLiteral(0));
    shouldParseNumericLiteral(10, new IntegerLiteral(10));
    shouldParseNumericLiteral(Integer.MAX_VALUE, new IntegerLiteral(Integer.MAX_VALUE));
  }

  @Test
  public void shouldParseLongLiterals() {
    shouldParseNumericLiteral(Integer.MAX_VALUE + 100L, new LongLiteral(Integer.MAX_VALUE + 100L));
  }

  @Test
  public void shouldParseNegativeInteger() {
    final String queryStr = String.format("SELECT -12345 FROM test1;");
    final Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    assertThat(statement, instanceOf(Query.class));
    final Query query = (Query) statement;
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    final QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
    final SingleColumn column0
        = (SingleColumn) querySpecification.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias().get(), equalTo("KSQL_COL_0"));
    assertThat(column0.getExpression(), instanceOf(ArithmeticUnaryExpression.class));
    final ArithmeticUnaryExpression aue = (ArithmeticUnaryExpression) column0.getExpression();
    assertThat(aue.getValue(), instanceOf(IntegerLiteral.class));
    assertThat(((IntegerLiteral) aue.getValue()).getValue(), equalTo(12345));
    assertThat(aue.getSign(), equalTo(ArithmeticUnaryExpression.Sign.MINUS));
  }

  @Test
  public void testBooleanLogicalExpression() {
    String
        queryStr =
        "SELECT 10, col2, 'test', 2.5, true, -5 FROM test1 WHERE col1 = 10 AND col2 LIKE 'val' OR col4 > 2.6 ;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    Assert.assertTrue("testSimpleQuery fails", statement instanceof Query);
    Query query = (Query) statement;
    Assert.assertTrue("testProjection fails", query.getQueryBody() instanceof QuerySpecification);
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
    Assert.assertTrue("testProjection fails", column0.getAlias().get().equalsIgnoreCase("KSQL_COL_0"));
    Assert.assertTrue("testProjection fails", column0.getExpression().toString().equalsIgnoreCase("10"));

    SingleColumn column1 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(1);
    Assert.assertTrue("testProjection fails", column1.getAlias().get().equalsIgnoreCase("COL2"));
    Assert.assertTrue("testProjection fails", column1.getExpression().toString().equalsIgnoreCase("TEST1.COL2"));

    SingleColumn column2 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(2);
    Assert.assertTrue("testProjection fails", column2.getAlias().get().equalsIgnoreCase("KSQL_COL_2"));
    Assert.assertTrue("testProjection fails", column2.getExpression().toString().equalsIgnoreCase("'test'"));

  }

  @Test
  public void shouldParseStructFieldAccessCorrectly() {
    final String simpleQuery = "SELECT iteminfo->category->name, address->street FROM orders WHERE address->state = 'CA';";
    final Statement statement = KSQL_PARSER.buildAst(simpleQuery, metaStore).get(0);


    Assert.assertTrue("testSimpleQuery fails", statement instanceof Query);
    final Query query = (Query) statement;
    assertThat("testSimpleQuery fails", query.getQueryBody(), instanceOf(QuerySpecification.class));
    final QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    assertThat("testSimpleQuery fails", querySpecification.getSelect().getSelectItems().size(), equalTo(2));
    final SingleColumn singleColumn0 = (SingleColumn) querySpecification.getSelect().getSelectItems().get(0);
    final SingleColumn singleColumn1 = (SingleColumn) querySpecification.getSelect().getSelectItems().get(1);
    assertThat(singleColumn0.getExpression(), instanceOf(FunctionCall.class));
    final FunctionCall functionCall0 = (FunctionCall) singleColumn0.getExpression();
    assertThat(functionCall0.toString(), equalTo("FETCH_FIELD_FROM_STRUCT(FETCH_FIELD_FROM_STRUCT(ORDERS.ITEMINFO, 'CATEGORY'), 'NAME')"));

    final FunctionCall functionCall1 = (FunctionCall) singleColumn1.getExpression();
    assertThat(functionCall1.toString(), equalTo("FETCH_FIELD_FROM_STRUCT(ORDERS.ADDRESS, 'STREET')"));

  }

  @Test
  public void testSimpleLeftJoin() {
    String
        queryStr =
        "SELECT t1.col1, t2.col1, t2.col4, col5, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON "
        + "t1.col1 = t2.col1;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    Assert.assertTrue("testSimpleQuery fails", statement instanceof Query);
    Query query = (Query) statement;
    Assert.assertTrue("testSimpleLeftJoin fails", query.getQueryBody() instanceof QuerySpecification);
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    Assert.assertTrue("testSimpleLeftJoin fails", querySpecification.getFrom() instanceof Join);
    Join join = (Join) querySpecification.getFrom();
    Assert.assertTrue("testSimpleLeftJoin fails", join.getType().toString().equalsIgnoreCase("LEFT"));

    Assert.assertTrue("testSimpleLeftJoin fails", ((AliasedRelation)join.getLeft()).getAlias().equalsIgnoreCase("T1"));
    Assert.assertTrue("testSimpleLeftJoin fails", ((AliasedRelation)join.getRight()).getAlias().equalsIgnoreCase("T2"));

  }

  @Test
  public void testLeftJoinWithFilter() {
    String
        queryStr =
        "SELECT t1.col1, t2.col1, t2.col4, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON t1.col1 = "
        + "t2.col1 WHERE t2.col2 = 'test';";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    Assert.assertTrue("testSimpleQuery fails", statement instanceof Query);
    Query query = (Query) statement;
    Assert.assertTrue("testLeftJoinWithFilter fails", query.getQueryBody() instanceof QuerySpecification);
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    Assert.assertTrue("testLeftJoinWithFilter fails", querySpecification.getFrom() instanceof Join);
    Join join = (Join) querySpecification.getFrom();
    Assert.assertTrue("testLeftJoinWithFilter fails", join.getType().toString().equalsIgnoreCase("LEFT"));

    Assert.assertTrue("testLeftJoinWithFilter fails", ((AliasedRelation)join.getLeft()).getAlias().equalsIgnoreCase("T1"));
    Assert.assertTrue("testLeftJoinWithFilter fails", ((AliasedRelation)join.getRight()).getAlias().equalsIgnoreCase("T2"));

    Assert.assertTrue("testLeftJoinWithFilter fails", querySpecification.getWhere().get().toString().equalsIgnoreCase("(T2.COL2 = 'test')"));
  }

  @Test
  public void testSelectAll() {
    String queryStr = "SELECT * FROM test1 t1;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    Assert.assertTrue("testSelectAll fails", statement instanceof Query);
    Query query = (Query) statement;
    Assert.assertTrue("testSelectAll fails", query.getQueryBody() instanceof QuerySpecification);
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    Assert.assertTrue("testSelectAll fails", querySpecification.getSelect().getSelectItems()
                                                 .size() == 8);
  }

  @Test
  public void testSelectAllJoin() {
    String
        queryStr =
        "SELECT * FROM test1 t1 LEFT JOIN test2 t2 ON t1.col1 = t2.col1 WHERE t2.col2 = 'test';";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    Assert.assertTrue("testSimpleQuery fails", statement instanceof Query);
    Query query = (Query) statement;
    Assert.assertTrue("testLeftJoinWithFilter fails", query.getQueryBody() instanceof QuerySpecification);
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();
    Assert.assertTrue("testSelectAllJoin fails", querySpecification.getFrom() instanceof Join);
    Join join = (Join) querySpecification.getFrom();
    Assert.assertTrue("testSelectAllJoin fails", querySpecification.getSelect().getSelectItems
        ().size() == 15);
    Assert.assertTrue("testLeftJoinWithFilter fails", ((AliasedRelation)join.getLeft()).getAlias().equalsIgnoreCase("T1"));
    Assert.assertTrue("testLeftJoinWithFilter fails", ((AliasedRelation)join.getRight()).getAlias().equalsIgnoreCase("T2"));
  }

  @Test
  public void testUDF() {
    String queryStr = "SELECT lcase(col1), concat(col2,'hello'), floor(abs(col3)) FROM test1 t1;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    Assert.assertTrue("testSelectAll fails", statement instanceof Query);
    Query query = (Query) statement;
    Assert.assertTrue("testSelectAll fails", query.getQueryBody() instanceof QuerySpecification);
    QuerySpecification querySpecification = (QuerySpecification)query.getQueryBody();

    SingleColumn column0 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(0);
    Assert.assertTrue("testProjection fails", column0.getAlias().get().equalsIgnoreCase("KSQL_COL_0"));
    Assert.assertTrue("testProjection fails", column0.getExpression().toString().equalsIgnoreCase("LCASE(T1.COL1)"));

    SingleColumn column1 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(1);
    Assert.assertTrue("testProjection fails", column1.getAlias().get().equalsIgnoreCase("KSQL_COL_1"));
    Assert.assertTrue("testProjection fails", column1.getExpression().toString().equalsIgnoreCase("CONCAT(T1.COL2, 'hello')"));

    SingleColumn column2 = (SingleColumn)querySpecification.getSelect().getSelectItems().get(2);
    Assert.assertTrue("testProjection fails", column2.getAlias().get().equalsIgnoreCase("KSQL_COL_2"));
    Assert.assertTrue("testProjection fails", column2.getExpression().toString().equalsIgnoreCase("FLOOR(ABS(T1.COL3))"));
  }

  @Test
  public void testRegisterTopic() {
    String
        queryStr =
        "REGISTER TOPIC orders_topic WITH (value_format = 'avro', "
        + "avroschemafile='/Users/hojjat/avro_order_schema.avro',kafka_topic='orders_topic');";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    Assert.assertTrue("testRegisterTopic failed.", statement instanceof RegisterTopic);
    RegisterTopic registerTopic = (RegisterTopic)statement;
    Assert.assertTrue("testRegisterTopic failed.", registerTopic
        .getName().toString().equalsIgnoreCase("ORDERS_TOPIC"));
    Assert.assertTrue("testRegisterTopic failed.", registerTopic.getProperties().size() == 3);
    Assert.assertTrue("testRegisterTopic failed.", registerTopic.getProperties().get(DdlConfig.VALUE_FORMAT_PROPERTY).toString().equalsIgnoreCase("'avro'"));
  }

  @Test
  public void testCreateStreamWithTopic() {
    String
        queryStr =
        "CREATE STREAM orders (ordertime bigint, orderid varchar, itemid varchar, orderunits "
        + "double) WITH (registered_topic = 'orders_topic' , key='ordertime');";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    Assert.assertTrue("testCreateStream failed.", statement instanceof CreateStream);
    CreateStream createStream = (CreateStream)statement;
    Assert.assertTrue("testCreateStream failed.", createStream.getName().toString().equalsIgnoreCase("ORDERS"));
    Assert.assertTrue("testCreateStream failed.", createStream.getElements().size() == 4);
    Assert.assertTrue("testCreateStream failed.", createStream.getElements().get(0).getName().toString().equalsIgnoreCase("ordertime"));
    Assert.assertTrue("testCreateStream failed.", createStream.getProperties().get(DdlConfig.TOPIC_NAME_PROPERTY).toString().equalsIgnoreCase("'orders_topic'"));
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
    Assert.assertTrue("testCreateStream failed.", statement instanceof CreateStream);
    CreateStream createStream = (CreateStream)statement;
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
    Assert.assertTrue("testCreateStream failed.", statement instanceof CreateStream);
    CreateStream createStream = (CreateStream)statement;
    Assert.assertTrue("testCreateStream failed.", createStream.getName().toString().equalsIgnoreCase("ORDERS"));
    Assert.assertTrue("testCreateStream failed.", createStream.getElements().size() == 4);
    Assert.assertTrue("testCreateStream failed.", createStream.getElements().get(0).getName().toString().equalsIgnoreCase("ordertime"));
    Assert.assertTrue("testCreateStream failed.", createStream.getProperties().get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY).toString().equalsIgnoreCase("'orders_topic'"));
    Assert.assertTrue("testCreateStream failed.", createStream.getProperties().get(DdlConfig
                                                                                       .VALUE_FORMAT_PROPERTY).toString().equalsIgnoreCase("'avro'"));
    Assert.assertTrue("testCreateStream failed.", createStream.getProperties().get(DdlConfig.AVRO_SCHEMA_FILE).toString().equalsIgnoreCase("'/Users/hojjat/avro_order_schema.avro'"));
  }

  @Test
  public void testCreateTableWithTopic() {
    String
        queryStr =
        "CREATE TABLE users (usertime bigint, userid varchar, regionid varchar, gender varchar) WITH (registered_topic = 'users_topic', key='userid', statestore='user_statestore');";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    Assert.assertTrue("testRegisterTopic failed.", statement instanceof CreateTable);
    CreateTable createTable = (CreateTable)statement;
    Assert.assertTrue("testCreateTable failed.", createTable.getName().toString().equalsIgnoreCase("USERS"));
    Assert.assertTrue("testCreateTable failed.", createTable.getElements().size() == 4);
    Assert.assertTrue("testCreateTable failed.", createTable.getElements().get(0).getName().toString().equalsIgnoreCase("usertime"));
    Assert.assertTrue("testCreateTable failed.", createTable.getProperties().get(DdlConfig.TOPIC_NAME_PROPERTY).toString().equalsIgnoreCase("'users_topic'"));
  }

  @Test
  public void testCreateTable() {
    String
        queryStr =
        "CREATE TABLE users (usertime bigint, userid varchar, regionid varchar, gender varchar) "
        + "WITH (kafka_topic = 'users_topic', value_format='json', key = 'userid');";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    Assert.assertTrue("testRegisterTopic failed.", statement instanceof CreateTable);
    CreateTable createTable = (CreateTable)statement;
    Assert.assertTrue("testCreateTable failed.", createTable.getName().toString().equalsIgnoreCase("USERS"));
    Assert.assertTrue("testCreateTable failed.", createTable.getElements().size() == 4);
    Assert.assertTrue("testCreateTable failed.", createTable.getElements().get(0).getName().toString().equalsIgnoreCase("usertime"));
    Assert.assertTrue("testCreateTable failed.", createTable.getProperties().get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY)
        .toString().equalsIgnoreCase("'users_topic'"));
    Assert.assertTrue("testCreateTable failed.", createTable.getProperties().get(DdlConfig.VALUE_FORMAT_PROPERTY)
        .toString().equalsIgnoreCase("'json'"));
  }

  @Test
  public void testCreateStreamAsSelect() {
    final String queryStr =
        "CREATE STREAM bigorders_json WITH (value_format = 'json', "
        + "kafka_topic='bigorders_topic') AS SELECT * FROM orders WHERE orderunits > 5 ;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    assertThat( statement, instanceOf(CreateStreamAsSelect.class));
    CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect)statement;
    assertThat(createStreamAsSelect.getName().toString().toLowerCase(), equalTo("bigorders_json"));
    assertThat(createStreamAsSelect.getQuery().getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification) createStreamAsSelect.getQuery().getQueryBody();
    assertThat(querySpecification.getSelect().getSelectItems().size(), equalTo(8));
    assertThat(querySpecification.getWhere().get().toString().toUpperCase(), equalTo("(ORDERS.ORDERUNITS > 5)"));
    assertThat(((AliasedRelation)querySpecification.getFrom()).getAlias().toUpperCase(), equalTo("ORDERS"));
  }

  @Test
  /*
      TODO: Handle so-called identifier expressions as values in table properties (right now, the lack of single quotes
      around in the variables <format> and <kafkaTopic> cause things to break).
   */
  @Ignore
  public void testCreateTopicFormatWithoutQuotes() {
    String ksqlTopic = "unquoted_topic";
    String format = "json";
    String kafkaTopic = "case_insensitive_kafka_topic";

    String queryStr = String.format(
        "REGISTER TOPIC %s WITH (value_format = %s, kafka_topic = %s);",
        ksqlTopic,
        format,
        kafkaTopic
    );
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    Assert.assertTrue(statement instanceof RegisterTopic);
    RegisterTopic registerTopic = (RegisterTopic) statement;
    Assert.assertTrue(registerTopic.getName().toString().equalsIgnoreCase(ksqlTopic));
    Assert.assertTrue(registerTopic.getProperties().size() == 2);
    Assert.assertTrue(registerTopic
                          .getProperties().get(DdlConfig.VALUE_FORMAT_PROPERTY).toString().equalsIgnoreCase(format));
    Assert.assertTrue(registerTopic.getProperties().get(DdlConfig.KAFKA_TOPIC_NAME_PROPERTY).toString().equalsIgnoreCase(kafkaTopic));
  }

  @Test
  public void testShouldFailIfWrongKeyword() {
    try {
      String simpleQuery = "SELLECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
      Statement statement = KSQL_PARSER.buildAst(simpleQuery, metaStore).get(0);
      Assert.fail();
    } catch (ParseFailedException e) {
      String errorMessage = e.getMessage();
      Assert.assertTrue(errorMessage.toLowerCase().contains(("line 1:1: mismatched input 'SELLECT'" + " expecting").toLowerCase()));
    }
  }

  @Test
  public void testSelectTumblingWindow() {

    String
        queryStr =
        "select itemid, sum(orderunits) from orders window TUMBLING ( size 30 second) where orderunits > 5 group by itemid;";
    Statement statement = KSQL_PARSER.buildAst(queryStr, metaStore).get(0);
    Assert.assertTrue("testSelectTumblingWindow failed.", statement instanceof Query);
    Query query = (Query) statement;
    Assert.assertTrue("testSelectTumblingWindow failed.", query.getQueryBody() instanceof QuerySpecification);
    QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
    Assert.assertTrue("testCreateTable failed.", querySpecification.getSelect().getSelectItems
        ().size() == 2);
    Assert.assertTrue("testSelectTumblingWindow failed.", querySpecification.getWhere().get().toString().equalsIgnoreCase("(ORDERS.ORDERUNITS > 5)"));
    Assert.assertTrue("testSelectTumblingWindow failed.", ((AliasedRelation)querySpecification.getFrom()).getAlias().equalsIgnoreCase("ORDERS"));
    Assert.assertTrue("testSelectTumblingWindow failed.", querySpecification
                                                               .getWindowExpression().isPresent());
    Assert.assertTrue("testSelectTumblingWindow failed.", querySpecification
        .getWindowExpression().get().toString().equalsIgnoreCase(" WINDOW STREAMWINDOW  TUMBLING ( SIZE 30 SECONDS ) "));
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
    assertThat(statement, instanceOf(Query.class));
    Query query = (Query) statement;
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
    assertThat(querySpecification.getSelect().getSelectItems().size(), equalTo(2));
    assertThat(querySpecification.getWhere().get().toString(), equalTo("(ORDERS.ORDERUNITS > 5)"));
    assertThat(((AliasedRelation)querySpecification.getFrom()).getAlias().toUpperCase(), equalTo("ORDERS"));
    Assert.assertTrue("window expression isn't present", querySpecification
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
    Assert.assertTrue("testSelectSessionWindow failed.", statement instanceof Query);
    Query query = (Query) statement;
    Assert.assertTrue("testSelectSessionWindow failed.", query.getQueryBody() instanceof QuerySpecification);
    QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
    Assert.assertTrue("testCreateTable failed.", querySpecification.getSelect().getSelectItems
        ().size() == 2);
    Assert.assertTrue("testSelectSessionWindow failed.", querySpecification.getWhere().get().toString().equalsIgnoreCase("(ORDERS.ORDERUNITS > 5)"));
    Assert.assertTrue("testSelectSessionWindow failed.", ((AliasedRelation)querySpecification.getFrom()).getAlias().equalsIgnoreCase("ORDERS"));
    Assert.assertTrue("testSelectSessionWindow failed.", querySpecification
        .getWindowExpression().isPresent());
    Assert.assertTrue("testSelectSessionWindow failed.", querySpecification
        .getWindowExpression().get().toString().equalsIgnoreCase(" WINDOW STREAMWINDOW  SESSION "
                                                                 + "( 30 SECONDS ) "));
  }

  @Test
  public void testShowTopics() {
    String simpleQuery = "SHOW TOPICS;";
    Statement statement = KSQL_PARSER.buildAst(simpleQuery, metaStore).get(0);
    Assert.assertTrue(statement instanceof ListTopics);
    ListTopics listTopics = (ListTopics) statement;
    Assert.assertTrue(listTopics.toString().equalsIgnoreCase("ListTopics{}"));
  }

  @Test
  public void testShowStreams() {
    String simpleQuery = "SHOW STREAMS;";
    Statement statement = KSQL_PARSER.buildAst(simpleQuery, metaStore).get(0);
    Assert.assertTrue(statement instanceof ListStreams);
    ListStreams listStreams = (ListStreams) statement;
    Assert.assertTrue(listStreams.toString().equalsIgnoreCase("ListStreams{}"));
    Assert.assertThat(listStreams.getShowExtended(), is(false));
  }

  @Test
  public void testShowTables() {
    String simpleQuery = "SHOW TABLES;";
    Statement statement = KSQL_PARSER.buildAst(simpleQuery, metaStore).get(0);
    Assert.assertTrue(statement instanceof ListTables);
    ListTables listTables = (ListTables) statement;
    Assert.assertTrue(listTables.toString().equalsIgnoreCase("ListTables{}"));
    Assert.assertThat(listTables.getShowExtended(), is(false));
  }

  @Test
  public void shouldReturnListQueriesForShowQueries() {
    String statementString = "SHOW QUERIES;";
    Statement statement = KSQL_PARSER.buildAst(statementString, metaStore).get(0);
    Assert.assertThat(statement, instanceOf(ListQueries.class));
    ListQueries listQueries = (ListQueries)statement;
    Assert.assertThat(listQueries.getShowExtended(), is(false));
  }

  @Test
  public void testShowProperties() {
    String simpleQuery = "SHOW PROPERTIES;";
    Statement statement = KSQL_PARSER.buildAst(simpleQuery, metaStore).get(0);
    Assert.assertTrue(statement instanceof ListProperties);
    ListProperties listProperties = (ListProperties) statement;
    Assert.assertTrue(listProperties.toString().equalsIgnoreCase("ListProperties{}"));
  }

  @Test
  public void testSetProperties() {
    String simpleQuery = "set 'auto.offset.reset'='earliest';";
    Statement statement = KSQL_PARSER.buildAst(simpleQuery, metaStore).get(0);
    Assert.assertTrue(statement instanceof SetProperty);
    SetProperty setProperty = (SetProperty) statement;
    Assert.assertTrue(setProperty.toString().equalsIgnoreCase("SetProperty{}"));
    Assert.assertTrue(setProperty.getPropertyName().equalsIgnoreCase("auto.offset.reset"));
    Assert.assertTrue(setProperty.getPropertyValue().equalsIgnoreCase("earliest"));
  }

  @Test
  public void testSelectSinkProperties() {
    String simpleQuery = "create stream s1 with (timestamp='orderid', partitions = 3) as select "
                         + "col1, col2"
                         + " from orders where col2 is null and col3 is not null or (col3*col2 = "
                         + "12);";
    Statement statement = KSQL_PARSER.buildAst(simpleQuery, metaStore).get(0);
    Assert.assertTrue("testSelectTumblingWindow failed.", statement instanceof CreateStreamAsSelect);
    CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;
    Assert.assertTrue("testSelectTumblingWindow failed.", createStreamAsSelect.getQuery().getQueryBody()
        instanceof QuerySpecification);
    QuerySpecification querySpecification = (QuerySpecification)
        createStreamAsSelect.getQuery().getQueryBody();
    Assert.assertTrue(querySpecification.getWhere().toString().equalsIgnoreCase("Optional[(((ORDERS.COL2 IS NULL) AND (ORDERS.COL3 IS NOT NULL)) OR ((ORDERS.COL3 * ORDERS.COL2) = 12))]"));
  }

  @Test
  public void shouldParseDropStream() {
    String simpleQuery = "DROP STREAM STREAM1;";
    List<Statement> statements =  KSQL_PARSER.buildAst(simpleQuery, metaStore);
    Statement statement =statements.get(0);
    assertThat(statement, instanceOf(DropStream.class));
    DropStream dropStream = (DropStream)  statement;
    assertThat(dropStream.getName().toString().toUpperCase(), equalTo("STREAM1"));
    assertThat(dropStream.getIfExists(), is(false));
  }

  @Test
  public void shouldParseDropTable() {
    String simpleQuery = "DROP TABLE TABLE1;";
    List<Statement> statements =  KSQL_PARSER.buildAst(simpleQuery, metaStore);
    Statement statement =statements.get(0);
    assertThat(statement, instanceOf(DropTable.class));
    DropTable dropTable = (DropTable)  statement;
    assertThat(dropTable.getName().toString().toUpperCase(), equalTo("TABLE1"));
    assertThat(dropTable.getIfExists(), is(false));
  }

  @Test
  public void shouldParseDropStreamIfExists() {
    String simpleQuery = "DROP STREAM IF EXISTS STREAM1;";
    List<Statement> statements =  KSQL_PARSER.buildAst(simpleQuery, metaStore);
    Statement statement =statements.get(0);
    assertThat(statement, instanceOf(DropStream.class));
    DropStream dropStream = (DropStream)  statement;
    assertThat(dropStream.getName().toString().toUpperCase(), equalTo("STREAM1"));
    assertThat(dropStream.getIfExists(), is(true));
  }

  @Test
  public void shouldParseDropTableIfExists() {
    String simpleQuery = "DROP TABLE IF EXISTS TABLE1;";
    List<Statement> statements =  KSQL_PARSER.buildAst(simpleQuery, metaStore);
    Statement statement =statements.get(0);
    assertThat(statement, instanceOf(DropTable.class));
    DropTable dropTable = (DropTable)  statement;
    assertThat(dropTable.getName().toString().toUpperCase(), equalTo("TABLE1"));
    assertThat(dropTable.getIfExists(), is(true));
  }

  @Test
  public void testInsertInto() {
    String insertIntoString = "INSERT INTO test2 SELECT col0, col2, col3 FROM test1 WHERE col0 > "
                            + "100;";
    Statement statement = KSQL_PARSER.buildAst(insertIntoString, metaStore).get(0);


    assertThat(statement, instanceOf(InsertInto.class));
    InsertInto insertInto = (InsertInto) statement;
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

  @Test
  public void shouldSetShowDescriptionsForShowStreamsDescriptions() {
    String statementString = "SHOW STREAMS EXTENDED;";
    Statement statement = KSQL_PARSER.buildAst(statementString, metaStore).get(0);
    Assert.assertThat(statement, instanceOf(ListStreams.class));
    ListStreams listStreams = (ListStreams)statement;
    Assert.assertThat(listStreams.getShowExtended(), is(true));
  }

  @Test
  public void shouldSetShowDescriptionsForShowTablesDescriptions() {
    String statementString = "SHOW TABLES EXTENDED;";
    Statement statement = KSQL_PARSER.buildAst(statementString, metaStore).get(0);
    Assert.assertThat(statement, instanceOf(ListTables.class));
    ListTables listTables = (ListTables)statement;
    Assert.assertThat(listTables.getShowExtended(), is(true));
  }

  @Test
  public void shouldSetShowDescriptionsForShowQueriesDescriptions() {
    String statementString = "SHOW QUERIES EXTENDED;";
    Statement statement = KSQL_PARSER.buildAst(statementString, metaStore).get(0);
    Assert.assertThat(statement, instanceOf(ListQueries.class));
    ListQueries listQueries = (ListQueries)statement;
    Assert.assertThat(listQueries.getShowExtended(), is(true));
  }

  @Test
  public void shouldSetWithinExpressionWithSingleWithin() {
    final String statementString = "CREATE STREAM foobar as SELECT * from TEST1 JOIN ORDERS WITHIN "
                                   + "10 SECONDS ON TEST1.col1 = ORDERS.ORDERID ;";

    final Statement statement = KSQL_PARSER.buildAst(statementString, metaStore).get(0);

    assertThat(statement, instanceOf(CreateStreamAsSelect.class));

    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;
    assertThat(createStreamAsSelect.getQuery().getQueryBody(),
               instanceOf(QuerySpecification.class));

    final QuerySpecification specification =
        (QuerySpecification) createStreamAsSelect.getQuery().getQueryBody();

    assertThat(specification.getFrom(), instanceOf(Join.class));

    final Join join = (Join) specification.getFrom();

    assertTrue(join.getWithinExpression().isPresent());

    final WithinExpression withinExpression = join.getWithinExpression().get();

    assertEquals(10L, withinExpression.getBefore());
    assertEquals(10L, withinExpression.getAfter());
    assertEquals(TimeUnit.SECONDS, withinExpression.getBeforeTimeUnit());
    assertEquals(Join.Type.INNER, join.getType());
  }


  @Test
  public void shouldSetWithinExpressionWithBeforeAndAfter() {
    final String statementString = "CREATE STREAM foobar as SELECT * from TEST1 JOIN ORDERS "
                                   + "WITHIN (10 seconds, 20 minutes) "
                                   + "ON TEST1.col1 = ORDERS.ORDERID ;";

    final Statement statement = KSQL_PARSER.buildAst(statementString, metaStore).get(0);

    assertThat(statement, instanceOf(CreateStreamAsSelect.class));

    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;
    assertThat(createStreamAsSelect.getQuery().getQueryBody(),
               instanceOf(QuerySpecification.class));

    final QuerySpecification specification =
        (QuerySpecification) createStreamAsSelect.getQuery().getQueryBody();

    assertThat(specification.getFrom(), instanceOf(Join.class));

    final Join join = (Join) specification.getFrom();

    assertTrue(join.getWithinExpression().isPresent());

    final WithinExpression withinExpression = join.getWithinExpression().get();

    assertEquals(10L, withinExpression.getBefore());
    assertEquals(20L, withinExpression.getAfter());
    assertEquals(TimeUnit.SECONDS, withinExpression.getBeforeTimeUnit());
    assertEquals(TimeUnit.MINUTES, withinExpression.getAfterTimeUnit());
    assertEquals(Join.Type.INNER, join.getType());
  }

  @Test
  public void shouldHaveInnerJoinTypeWithExplicitInnerKeyword() {
    final String statementString = "CREATE STREAM foobar as SELECT * from TEST1 INNER JOIN TEST2 "
                                   + "ON TEST1.col1 = TEST2.col1;";

    final Statement statement = KSQL_PARSER.buildAst(statementString, metaStore).get(0);

    assertThat(statement, instanceOf(CreateStreamAsSelect.class));

    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;
    assertThat(createStreamAsSelect.getQuery().getQueryBody(),
               instanceOf(QuerySpecification.class));

    final QuerySpecification specification =
        (QuerySpecification) createStreamAsSelect.getQuery().getQueryBody();

    assertThat(specification.getFrom(), instanceOf(Join.class));

    final Join join = (Join) specification.getFrom();

    assertEquals(Join.Type.INNER, join.getType());
  }

  @Test
  public void shouldHaveLeftJoinTypeWhenOuterIsSpecified() {
    final String statementString = "CREATE STREAM foobar as SELECT * from TEST1 LEFT OUTER JOIN "
                                   + "TEST2 ON TEST1.col1 = TEST2.col1;";

    final Statement statement = KSQL_PARSER.buildAst(statementString, metaStore).get(0);

    assertThat(statement, instanceOf(CreateStreamAsSelect.class));

    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;
    assertThat(createStreamAsSelect.getQuery().getQueryBody(),
               instanceOf(QuerySpecification.class));

    final QuerySpecification specification =
        (QuerySpecification) createStreamAsSelect.getQuery().getQueryBody();

    assertThat(specification.getFrom(), instanceOf(Join.class));

    final Join join = (Join) specification.getFrom();

    assertEquals(Join.Type.LEFT, join.getType());
  }

  @Test
  public void shouldHaveLeftJoinType() {
    final String statementString = "CREATE STREAM foobar as SELECT * from TEST1 LEFT JOIN "
                                   + "TEST2 ON TEST1.col1 = TEST2.col1;";

    final Statement statement = KSQL_PARSER.buildAst(statementString, metaStore).get(0);

    assertThat(statement, instanceOf(CreateStreamAsSelect.class));

    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;
    assertThat(createStreamAsSelect.getQuery().getQueryBody(),
               instanceOf(QuerySpecification.class));

    final QuerySpecification specification =
        (QuerySpecification) createStreamAsSelect.getQuery().getQueryBody();

    assertThat(specification.getFrom(), instanceOf(Join.class));

    final Join join = (Join) specification.getFrom();

    assertEquals(Join.Type.LEFT, join.getType());
  }

  @Test
  public void shouldHaveOuterJoinType() {
    final String statementString = "CREATE STREAM foobar as SELECT * from TEST1 FULL JOIN "
                                   + "TEST2 ON TEST1.col1 = TEST2.col1;";

    final Statement statement = KSQL_PARSER.buildAst(statementString, metaStore).get(0);

    assertThat(statement, instanceOf(CreateStreamAsSelect.class));

    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;
    assertThat(createStreamAsSelect.getQuery().getQueryBody(),
               instanceOf(QuerySpecification.class));

    final QuerySpecification specification =
        (QuerySpecification) createStreamAsSelect.getQuery().getQueryBody();

    assertThat(specification.getFrom(), instanceOf(Join.class));

    final Join join = (Join) specification.getFrom();

    assertEquals(Join.Type.OUTER, join.getType());
  }

  @Test
  public void shouldHaveOuterJoinTypeWhenOuterKeywordIsSpecified() {
    final String statementString = "CREATE STREAM foobar as SELECT * from TEST1 FULL OUTER JOIN "
                                   + "TEST2 ON TEST1.col1 = TEST2.col1;";

    final Statement statement = KSQL_PARSER.buildAst(statementString, metaStore).get(0);

    assertThat(statement, instanceOf(CreateStreamAsSelect.class));

    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;
    assertThat(createStreamAsSelect.getQuery().getQueryBody(),
               instanceOf(QuerySpecification.class));

    final QuerySpecification specification =
        (QuerySpecification) createStreamAsSelect.getQuery().getQueryBody();

    assertThat(specification.getFrom(), instanceOf(Join.class));

    final Join join = (Join) specification.getFrom();

    assertEquals(Join.Type.OUTER, join.getType());
  }

  @Test
  public void shouldAddPrefixEvenIfColumnNameIsTheSameAsStream() {
    final String statementString =
        "CREATE STREAM S AS SELECT address FROM address a;";
    final Statement statement = KSQL_PARSER.buildAst(statementString, metaStore).get(0);
    assertThat(statement, instanceOf(CreateStreamAsSelect.class));
    Query query = ((CreateStreamAsSelect) statement).getQuery();
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
    assertThat(querySpecification.getSelect().getSelectItems().get(0).toString(),
        equalTo("A.ADDRESS ADDRESS"));
  }

  @Test
  public void shouldNotAddPrefixIfStreamNameIsPrefix() {
    final String statementString =
        "CREATE STREAM S AS SELECT address.orderid FROM address a;";
    final List<Statement> statements = KSQL_PARSER.buildAst(statementString, metaStore);
    final Statement statement = KSQL_PARSER.buildAst(statementString, metaStore).get(0);
    assertThat(statement, instanceOf(CreateStreamAsSelect.class));
    Query query = ((CreateStreamAsSelect) statement).getQuery();
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
    assertThat(querySpecification.getSelect().getSelectItems().get(0).toString(),
        equalTo("ADDRESS.ORDERID ORDERID"));
  }

  @Test
  public void shouldPassIfStreamColumnNameWithAliasIsNotAmbiguous() {
    final String statementString =
        "CREATE STREAM S AS SELECT a.address->city FROM address a;";
    final Statement statement = KSQL_PARSER.buildAst(statementString, metaStore).get(0);
    assertThat(statement, instanceOf(CreateStreamAsSelect.class));
    Query query = ((CreateStreamAsSelect) statement).getQuery();
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
    assertThat(querySpecification.getSelect().getSelectItems().get(0).toString(),
        equalTo("FETCH_FIELD_FROM_STRUCT(A.ADDRESS, 'CITY') ADDRESS__CITY"));
  }

  @Test
  public void shouldPassIfStreamColumnNameIsNotAmbiguous() {
    final String statementString =
        "CREATE STREAM S AS SELECT address.address->city FROM address a;";
    final Statement statement = KSQL_PARSER.buildAst(statementString, metaStore).get(0);
    assertThat(statement, instanceOf(CreateStreamAsSelect.class));
    Query query = ((CreateStreamAsSelect) statement).getQuery();
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
    assertThat(querySpecification.getSelect().getSelectItems().get(0).toString(),
        equalTo("FETCH_FIELD_FROM_STRUCT(ADDRESS.ADDRESS, 'CITY') ADDRESS__CITY"));
  }

  @Test(expected = KsqlException.class)
  public void shouldFailJoinQueryParseIfStreamColumnNameWithNoAliasIsAmbiguous() {
    final String statementString =
        "CREATE STREAM S AS SELECT itemid FROM address a JOIN itemid on a.itemid = itemid.itemid;";
    final List<Statement> statements = KSQL_PARSER.buildAst(statementString, metaStore);
  }

  @Test
  public void shouldPassJoinQueryParseIfStreamColumnNameWithAliasIsNotAmbiguous() {
    final String statementString =
        "CREATE STREAM S AS SELECT itemid.itemid FROM address a JOIN itemid on a.itemid = itemid.itemid;";
    final Statement statement = KSQL_PARSER.buildAst(statementString, metaStore).get(0);
    assertThat(statement, instanceOf(CreateStreamAsSelect.class));
    Query query = ((CreateStreamAsSelect) statement).getQuery();
    assertThat(query.getQueryBody(), instanceOf(QuerySpecification.class));
    QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();
    assertThat(querySpecification.getSelect().getSelectItems().get(0).toString(), equalTo("ITEMID.ITEMID ITEMID_ITEMID"));
  }
}
