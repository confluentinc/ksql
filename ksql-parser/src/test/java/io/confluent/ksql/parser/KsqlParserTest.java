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

package io.confluent.ksql.parser;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import com.google.common.collect.Iterables;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.IntegerLiteral;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.parser.tree.ListQueries;
import io.confluent.ksql.parser.tree.ListStreams;
import io.confluent.ksql.parser.tree.ListTables;
import io.confluent.ksql.parser.tree.ListTopics;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.parser.tree.LongLiteral;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.SearchedCaseExpression;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.json.KsqlJsonSerdeFactory;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


public class KsqlParserTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private MutableMetaStore metaStore;


  @Before
  public void init() {

    metaStore = MetaStoreFixture.getNewMetaStore(mock(FunctionRegistry.class));

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
        .field("ITEMID", Schema.OPTIONAL_INT64_SCHEMA)
        .field("NAME", Schema.OPTIONAL_STRING_SCHEMA)
        .field("CATEGORY", categorySchema)
        .optional().build();

    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    final Schema schemaBuilderOrders = schemaBuilder
        .field("ORDERTIME", Schema.OPTIONAL_INT64_SCHEMA)
        .field("ORDERID", Schema.OPTIONAL_INT64_SCHEMA)
        .field("ITEMID", Schema.OPTIONAL_STRING_SCHEMA)
        .field("ITEMINFO", itemInfoSchema)
        .field("ORDERUNITS", Schema.OPTIONAL_INT32_SCHEMA)
        .field("ARRAYCOL",SchemaBuilder
            .array(Schema.OPTIONAL_FLOAT64_SCHEMA)
            .optional()
            .build())
        .field("MAPCOL", SchemaBuilder
            .map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA)
            .optional()
            .build())
        .field("ADDRESS", addressSchema)
        .build();

    final KsqlTopic ksqlTopicOrders =
        new KsqlTopic("ADDRESS_TOPIC", "orders_topic", new KsqlJsonSerdeFactory(), false);

    final KsqlStream ksqlStreamOrders = new KsqlStream<>(
        "sqlexpression",
        "ADDRESS",
        LogicalSchema.of(schemaBuilderOrders),
        SerdeOption.none(),
        KeyField.of("ORDERTIME", schemaBuilderOrders.field("ORDERTIME")),
        new MetadataTimestampExtractionPolicy(),
        ksqlTopicOrders,
        Serdes::String
    );

    metaStore.putSource(ksqlStreamOrders);

    final KsqlTopic ksqlTopicItems =
        new KsqlTopic("ITEMS_TOPIC", "item_topic", new KsqlJsonSerdeFactory(), false);

    final KsqlTable<String> ksqlTableOrders = new KsqlTable<>(
        "sqlexpression",
        "ITEMID",
        LogicalSchema.of(itemInfoSchema),
        SerdeOption.none(),
        KeyField.of("ITEMID", itemInfoSchema.field("ITEMID")),
        new MetadataTimestampExtractionPolicy(),
        ksqlTopicItems,
        Serdes::String
    );

    metaStore.putSource(ksqlTableOrders);
  }

  @Test
  public void testSimpleQuery() {
    final String simpleQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final PreparedStatement<?> statement = KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore);

    assertThat(statement.getStatementText(), is(simpleQuery));
    Assert.assertTrue(statement.getStatement() instanceof Query);
    final Query query = (Query) statement.getStatement();
    Assert.assertTrue(query.getSelect().getSelectItems().size() == 3);
    assertThat(query.getFrom(), not(nullValue()));
    Assert.assertTrue(query.getWhere().isPresent());
    Assert.assertTrue(query.getWhere().get() instanceof ComparisonExpression);
    final ComparisonExpression comparisonExpression = (ComparisonExpression)query.getWhere().get();
    Assert.assertTrue(comparisonExpression.getType().getValue().equalsIgnoreCase(">"));

  }

  @Test
  public void testProjection() {
    final String queryStr = "SELECT col0, col2, col3 FROM test1;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    Assert.assertTrue(statement instanceof Query);
    final Query query = (Query) statement;
    Assert.assertTrue(query.getSelect().getSelectItems().size() == 3);
    Assert.assertTrue(query.getSelect().getSelectItems().get(0) instanceof SingleColumn);
    final SingleColumn column0 = (SingleColumn)query.getSelect().getSelectItems().get(0);
    Assert.assertTrue(column0.getAlias().equalsIgnoreCase("COL0"));
    Assert.assertTrue(column0.getExpression().toString().equalsIgnoreCase("TEST1.COL0"));
  }

  @Test
  public void testProjectionWithArrayMap() {
    final String queryStr = "SELECT col0, col2, col3, col4[0], col5['key1'] FROM test1;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    Assert.assertTrue(statement instanceof Query);
    final Query query = (Query) statement;
    Assert.assertTrue(query.getSelect().getSelectItems()
                                                  .size() == 5);
    Assert.assertTrue(query.getSelect().getSelectItems().get(0) instanceof SingleColumn);
    final SingleColumn column0 = (SingleColumn)query.getSelect().getSelectItems().get(0);
    Assert.assertTrue("testProjectionWithArrayMap fails",
        column0.getAlias().equalsIgnoreCase("COL0"));
    Assert.assertTrue(column0.getExpression().toString().equalsIgnoreCase("TEST1.COL0"));

    final SingleColumn column3 = (SingleColumn)query.getSelect().getSelectItems().get(3);
    final SingleColumn column4 = (SingleColumn)query.getSelect().getSelectItems().get(4);
    Assert.assertTrue(column3.getExpression().toString()
        .equalsIgnoreCase("TEST1.COL4[0]"));
    Assert.assertTrue(column4.getExpression().toString()
        .equalsIgnoreCase("TEST1.COL5['key1']"));
  }

  @Test
  public void testProjectFilter() {
    final String queryStr = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    Assert.assertTrue(statement instanceof Query);
    final Query query = (Query) statement;

    Assert.assertTrue(query.getWhere().get() instanceof ComparisonExpression);
    final ComparisonExpression comparisonExpression = (ComparisonExpression)query.getWhere().get();
    Assert.assertTrue(comparisonExpression.toString().equalsIgnoreCase("(TEST1.COL0 > 100)"));
    Assert.assertTrue(query.getSelect().getSelectItems().size() == 3);

  }

  @Test
  public void testBinaryExpression() {
    final String queryStr = "SELECT col0+10, col2, col3-col1 FROM test1;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    Assert.assertTrue(statement instanceof Query);
    final Query query = (Query) statement;
    final SingleColumn column0 = (SingleColumn)query.getSelect().getSelectItems().get(0);
    Assert.assertTrue(column0.getAlias().equalsIgnoreCase("KSQL_COL_0"));
    Assert.assertTrue(column0.getExpression().toString().equalsIgnoreCase("(TEST1.COL0 + 10)"));
  }

  @Test
  public void testBooleanExpression() {
    final String queryStr = "SELECT col0 = 10, col2, col3 > col1 FROM test1;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    Assert.assertTrue(statement instanceof Query);
    final Query query = (Query) statement;
    final SingleColumn column0 = (SingleColumn)query.getSelect().getSelectItems().get(0);
    Assert.assertTrue(column0.getAlias().equalsIgnoreCase("KSQL_COL_0"));
    Assert.assertTrue(column0.getExpression().toString().equalsIgnoreCase("(TEST1.COL0 = 10)"));
  }

  @Test
  public void testLiterals() {
    final String queryStr = "SELECT 10, col2, 'test', 2.5, true, -5 FROM test1;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    Assert.assertTrue(statement instanceof Query);
    final Query query = (Query) statement;
    final SingleColumn column0 = (SingleColumn)query.getSelect().getSelectItems().get(0);
    Assert.assertTrue(column0.getAlias().equalsIgnoreCase("KSQL_COL_0"));
    Assert.assertTrue(column0.getExpression().toString().equalsIgnoreCase("10"));

    final SingleColumn column1 = (SingleColumn)query.getSelect().getSelectItems().get(1);
    Assert.assertTrue(column1.getAlias().equalsIgnoreCase("COL2"));
    Assert.assertTrue(column1.getExpression().toString().equalsIgnoreCase("TEST1.COL2"));

    final SingleColumn column2 = (SingleColumn)query.getSelect().getSelectItems().get(2);
    Assert.assertTrue(column2.getAlias().equalsIgnoreCase("KSQL_COL_2"));
    Assert.assertTrue(column2.getExpression().toString().equalsIgnoreCase("'test'"));

    final SingleColumn column3 = (SingleColumn)query.getSelect().getSelectItems().get(3);
    Assert.assertTrue(column3.getAlias().equalsIgnoreCase("KSQL_COL_3"));
    Assert.assertTrue(column3.getExpression().toString().equalsIgnoreCase("2.5"));

    final SingleColumn column4 = (SingleColumn)query.getSelect().getSelectItems().get(4);
    Assert.assertTrue(column4.getAlias().equalsIgnoreCase("KSQL_COL_4"));
    Assert.assertTrue(column4.getExpression().toString().equalsIgnoreCase("true"));

    final SingleColumn column5 = (SingleColumn)query.getSelect().getSelectItems().get(5);
    Assert.assertTrue(column5.getAlias().equalsIgnoreCase("KSQL_COL_5"));
    Assert.assertTrue(column5.getExpression().toString().equalsIgnoreCase("-5"));
  }

  private <T, L extends Literal> void shouldParseNumericLiteral(final T value,
                                                                final L expectedValue) {
    final String queryStr = String.format("SELECT " + value.toString() + " FROM test1;", value);
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    assertThat(statement, instanceOf(Query.class));
    final Query query = (Query) statement;
    final SingleColumn column0
        = (SingleColumn) query.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias(), equalTo("KSQL_COL_0"));
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
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    assertThat(statement, instanceOf(Query.class));
    final Query query = (Query) statement;
    final SingleColumn column0
        = (SingleColumn) query.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias(), equalTo("KSQL_COL_0"));
    assertThat(column0.getExpression(), instanceOf(ArithmeticUnaryExpression.class));
    final ArithmeticUnaryExpression aue = (ArithmeticUnaryExpression) column0.getExpression();
    assertThat(aue.getValue(), instanceOf(IntegerLiteral.class));
    assertThat(((IntegerLiteral) aue.getValue()).getValue(), equalTo(12345));
    assertThat(aue.getSign(), equalTo(ArithmeticUnaryExpression.Sign.MINUS));
  }

  @Test
  public void testBooleanLogicalExpression() {
    final String
        queryStr =
        "SELECT 10, col2, 'test', 2.5, true, -5 FROM test1 WHERE col1 = 10 AND col2 LIKE 'val' OR col4 > 2.6 ;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    Assert.assertTrue(statement instanceof Query);
    final Query query = (Query) statement;
    final SingleColumn column0 = (SingleColumn)query.getSelect().getSelectItems().get(0);
    Assert.assertTrue(column0.getAlias().equalsIgnoreCase("KSQL_COL_0"));
    Assert.assertTrue(column0.getExpression().toString().equalsIgnoreCase("10"));

    final SingleColumn column1 = (SingleColumn)query.getSelect().getSelectItems().get(1);
    Assert.assertTrue(column1.getAlias().equalsIgnoreCase("COL2"));
    Assert.assertTrue(column1.getExpression().toString().equalsIgnoreCase("TEST1.COL2"));

    final SingleColumn column2 = (SingleColumn)query.getSelect().getSelectItems().get(2);
    Assert.assertTrue(column2.getAlias().equalsIgnoreCase("KSQL_COL_2"));
    Assert.assertTrue(column2.getExpression().toString().equalsIgnoreCase("'test'"));

  }

  @Test
  public void shouldParseStructFieldAccessCorrectly() {
    final String simpleQuery = "SELECT iteminfo->category->name, address->street FROM orders WHERE address->state = 'CA';";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();


    Assert.assertTrue(statement instanceof Query);
    final Query query = (Query) statement;
    assertThat(query.getSelect().getSelectItems().size(), equalTo(2));
    final SingleColumn singleColumn0 = (SingleColumn) query.getSelect().getSelectItems().get(0);
    final SingleColumn singleColumn1 = (SingleColumn) query.getSelect().getSelectItems().get(1);
    assertThat(singleColumn0.getExpression(), instanceOf(FunctionCall.class));
    final FunctionCall functionCall0 = (FunctionCall) singleColumn0.getExpression();
    assertThat(functionCall0.toString(), equalTo("FETCH_FIELD_FROM_STRUCT(FETCH_FIELD_FROM_STRUCT(ORDERS.ITEMINFO, 'CATEGORY'), 'NAME')"));

    final FunctionCall functionCall1 = (FunctionCall) singleColumn1.getExpression();
    assertThat(functionCall1.toString(), equalTo("FETCH_FIELD_FROM_STRUCT(ORDERS.ADDRESS, 'STREET')"));

  }

  @Test
  public void testSimpleLeftJoin() {
    final String
        queryStr =
        "SELECT t1.col1, t2.col1, t2.col4, col5, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON "
        + "t1.col1 = t2.col1;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    Assert.assertTrue(statement instanceof Query);
    final Query query = (Query) statement;
    Assert.assertTrue("testSimpleLeftJoin fails", query.getFrom() instanceof Join);
    final Join join = (Join) query.getFrom();
    Assert.assertTrue("testSimpleLeftJoin fails", join.getType().toString().equalsIgnoreCase("LEFT"));

    Assert.assertTrue("testSimpleLeftJoin fails", ((AliasedRelation)join.getLeft()).getAlias().equalsIgnoreCase("T1"));
    Assert.assertTrue("testSimpleLeftJoin fails", ((AliasedRelation)join.getRight()).getAlias().equalsIgnoreCase("T2"));

  }

  @Test
  public void testLeftJoinWithFilter() {
    final String
        queryStr =
        "SELECT t1.col1, t2.col1, t2.col4, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON t1.col1 = "
        + "t2.col1 WHERE t2.col2 = 'test';";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    Assert.assertTrue(statement instanceof Query);
    final Query query = (Query) statement;
    Assert.assertTrue(query.getFrom() instanceof Join);
    final Join join = (Join) query.getFrom();
    Assert.assertTrue(join.getType().toString().equalsIgnoreCase("LEFT"));

    Assert.assertTrue(((AliasedRelation)join.getLeft()).getAlias().equalsIgnoreCase("T1"));
    Assert.assertTrue(((AliasedRelation)join.getRight()).getAlias().equalsIgnoreCase("T2"));

    Assert.assertTrue(query.getWhere().get().toString().equalsIgnoreCase("(T2.COL2 = 'test')"));
  }

  @Test
  public void testSelectAll() {
    // When:
    final Query query = KsqlParserTestUtil.<Query>buildSingleAst(
        "SELECT * FROM test1 t1;",
        metaStore).getStatement();

    assertThat(query.getSelect().getSelectItems(), is(contains(new AllColumns(Optional.empty()))));
  }

  @Test
  public void testReservedColumnIdentifers() {
    assertQuerySucceeds("SELECT ROWTIME as ROWTIME FROM test1 t1;");
    assertQuerySucceeds("SELECT ROWKEY as ROWKEY FROM test1 t1;");
  }

  @Test
  public void testReservedRowTimeAlias() {
    expectedException.expect(ParseFailedException.class);
    expectedException.expectMessage(containsString(
        "ROWTIME is a reserved token for implicit column. You cannot use it as an alias for a column."));

    KsqlParserTestUtil.buildSingleAst("SELECT C1 as ROWTIME FROM test1 t1;", metaStore);
  }

  @Test
  public void testReservedRowKeyAlias() {
    expectedException.expect(ParseFailedException.class);
    expectedException.expectMessage(containsString(
        "ROWKEY is a reserved token for implicit column. You cannot use it as an alias for a column."));

    KsqlParserTestUtil.buildSingleAst("SELECT C2 as ROWKEY FROM test1 t1;", metaStore);
  }

  @Test
  public void testSelectAllJoin() {
    // When:
    final Query query = KsqlParserTestUtil.<Query>buildSingleAst(
        "SELECT * FROM test1 t1 LEFT JOIN test2 t2 ON t1.col1 = t2.col1 WHERE t2.col2 = 'test';",
        metaStore)
        .getStatement();

    // Then:
    assertThat(query.getFrom(), is(instanceOf(Join.class)));
    assertThat(query.getSelect().getSelectItems(), is(contains(new AllColumns(Optional.empty()))));

    final Join join = (Join) query.getFrom();
    assertThat(((AliasedRelation) join.getLeft()).getAlias(), is("T1"));
    assertThat(((AliasedRelation) join.getRight()).getAlias(), is("T2"));
  }

  @Test
  public void testUDF() {
    final String queryStr = "SELECT lcase(col1), concat(col2,'hello'), floor(abs(col3)) FROM test1 t1;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    Assert.assertTrue("testSelectAll fails", statement instanceof Query);
    final Query query = (Query) statement;

    final SingleColumn column0 = (SingleColumn)query.getSelect().getSelectItems().get(0);
    Assert.assertTrue(column0.getAlias().equalsIgnoreCase("KSQL_COL_0"));
    Assert.assertTrue(column0.getExpression().toString().equalsIgnoreCase("LCASE(T1.COL1)"));

    final SingleColumn column1 = (SingleColumn)query.getSelect().getSelectItems().get(1);
    Assert.assertTrue(column1.getAlias().equalsIgnoreCase("KSQL_COL_1"));
    Assert.assertTrue(column1.getExpression().toString().equalsIgnoreCase("CONCAT(T1.COL2, 'hello')"));

    final SingleColumn column2 = (SingleColumn)query.getSelect().getSelectItems().get(2);
    Assert.assertTrue(column2.getAlias().equalsIgnoreCase("KSQL_COL_2"));
    Assert.assertTrue(column2.getExpression().toString().equalsIgnoreCase("FLOOR(ABS(T1.COL3))"));
  }

  @Test
  public void testCreateStream() {
    // When:
    final CreateStream result = (CreateStream) KsqlParserTestUtil.buildSingleAst("CREATE STREAM orders ("
        + "ordertime bigint, "
        + "orderid varchar, "
        + "itemid varchar, "
        + "orderunits double, "
        + "arraycol array<double>, "
        + "mapcol map<varchar, double>, "
        + "order_address STRUCT<number VARCHAR, street VARCHAR, zip INTEGER>"
        + ") WITH (value_format = 'avro',kafka_topic='orders_topic');", metaStore).getStatement();

    // Then:
    assertThat(result.getName().toString(), equalTo("ORDERS"));
    assertThat(Iterables.size(result.getElements()), equalTo(7));
    assertThat(Iterables.get(result.getElements(), 0).getName(), equalTo("ORDERTIME"));
    assertThat(Iterables.get(result.getElements(), 6).getType().getSqlType().baseType(), equalTo(SqlBaseType.STRUCT));
    assertThat(result.getProperties().getKafkaTopic(), equalTo("orders_topic"));
    assertThat(result.getProperties().getValueFormat(), equalTo(Format.AVRO));
  }

  @Test
  public void testCreateTable() {
    // When:
    final CreateTable result = (CreateTable) KsqlParserTestUtil.buildSingleAst(
        "CREATE TABLE users (usertime bigint, userid varchar, regionid varchar, gender varchar) "
            + "WITH (kafka_topic='foo', value_format='json', key='userid');", metaStore).getStatement();

    // Then:
    assertThat(result.getName().toString(), equalTo("USERS"));
    assertThat(Iterables.size(result.getElements()), equalTo(4));
    assertThat(Iterables.get(result.getElements(), 0).getName(), equalTo("USERTIME"));
    assertThat(result.getProperties().getKafkaTopic(), equalTo("foo"));
    assertThat(result.getProperties().getValueFormat(), equalTo(Format.JSON));
    assertThat(result.getProperties().getKeyField(), equalTo(Optional.of("userid")));
  }

  @Test
  public void testCreateStreamAsSelect() {
    // Given:
    final CreateStreamAsSelect csas = KsqlParserTestUtil.<CreateStreamAsSelect>buildSingleAst(
        "CREATE STREAM bigorders_json WITH (value_format = 'json', "
            + "kafka_topic='bigorders_topic') AS SELECT * FROM orders WHERE orderunits > 5 ;",
        metaStore)
        .getStatement();

    // Then:
    assertThat(csas.getName().toString().toLowerCase(), equalTo("bigorders_json"));
    final Query query = csas.getQuery();
    assertThat(query.getSelect().getSelectItems(), is(contains(new AllColumns(Optional.empty()))));
    assertThat(query.getWhere().get().toString().toUpperCase(), equalTo("(ORDERS.ORDERUNITS > 5)"));
    assertThat(((AliasedRelation)query.getFrom()).getAlias().toUpperCase(), equalTo("ORDERS"));
  }

  @Test
  public void testShouldFailIfWrongKeyword() {
    try {
      final String simpleQuery = "SELLECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
      KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore);
      fail(format("Expected query: %s to fail", simpleQuery));
    } catch (final ParseFailedException e) {
      final String errorMessage = e.getMessage();
      Assert.assertTrue(errorMessage.toLowerCase().contains(("line 1:1: mismatched input 'SELLECT'" + " expecting").toLowerCase()));
    }
  }

  @Test
  public void testSelectTumblingWindow() {

    final String
        queryStr =
        "select itemid, sum(orderunits) from orders window TUMBLING ( size 30 second) where orderunits > 5 group by itemid;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    Assert.assertTrue(statement instanceof Query);
    final Query query = (Query) statement;
    Assert.assertTrue(query.getSelect().getSelectItems
        ().size() == 2);
    Assert.assertTrue(query.getWhere().get().toString().equalsIgnoreCase("(ORDERS.ORDERUNITS > 5)"));
    Assert.assertTrue(((AliasedRelation)query.getFrom()).getAlias().equalsIgnoreCase("ORDERS"));
    Assert.assertTrue(query.getWindow().isPresent());
    Assert.assertTrue(query.getWindow().get().toString().equalsIgnoreCase(" WINDOW STREAMWINDOW  TUMBLING ( SIZE 30 SECONDS ) "));
  }

  @Test
  public void testSelectHoppingWindow() {

    final String
        queryStr =
        "select itemid, sum(orderunits) from orders window HOPPING ( size 30 second, advance by 5"
        + " seconds) "
        + "where "
        + "orderunits"
        + " > 5 group by itemid;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    assertThat(statement, instanceOf(Query.class));
    final Query query = (Query) statement;
    assertThat(query.getSelect().getSelectItems().size(), equalTo(2));
    assertThat(query.getWhere().get().toString(), equalTo("(ORDERS.ORDERUNITS > 5)"));
    assertThat(((AliasedRelation)query.getFrom()).getAlias().toUpperCase(), equalTo("ORDERS"));
    Assert.assertTrue("window expression isn't present", query
        .getWindow().isPresent());
    assertThat(query.getWindow().get().toString().toUpperCase(),
        equalTo(" WINDOW STREAMWINDOW  HOPPING ( SIZE 30 SECONDS , ADVANCE BY 5 SECONDS ) "));
  }

  @Test
  public void testSelectSessionWindow() {

    final String
        queryStr =
        "select itemid, sum(orderunits) from orders window SESSION ( 30 second) where "
        + "orderunits > 5 group by itemid;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    Assert.assertTrue(statement instanceof Query);
    final Query query = (Query) statement;
    Assert.assertTrue(query.getSelect().getSelectItems
        ().size() == 2);
    Assert.assertTrue(query.getWhere().get().toString().equalsIgnoreCase("(ORDERS.ORDERUNITS > 5)"));
    Assert.assertTrue(((AliasedRelation)query.getFrom()).getAlias().equalsIgnoreCase("ORDERS"));
    Assert.assertTrue(query
        .getWindow().isPresent());
    Assert.assertTrue(query
        .getWindow().get().toString().equalsIgnoreCase(" WINDOW STREAMWINDOW  SESSION "
                                                                 + "( 30 SECONDS ) "));
  }

  @Test
  public void testShowTopics() {
    final String simpleQuery = "SHOW TOPICS;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();
    Assert.assertTrue(statement instanceof ListTopics);
    final ListTopics listTopics = (ListTopics) statement;
    Assert.assertTrue(listTopics.toString().equalsIgnoreCase("ListTopics{}"));
  }

  @Test
  public void testShowStreams() {
    final String simpleQuery = "SHOW STREAMS;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();
    Assert.assertTrue(statement instanceof ListStreams);
    final ListStreams listStreams = (ListStreams) statement;
    assertThat(listStreams.toString(), is("ListStreams{showExtended=false}"));
    Assert.assertThat(listStreams.getShowExtended(), is(false));
  }

  @Test
  public void testShowTables() {
    final String simpleQuery = "SHOW TABLES;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();
    Assert.assertTrue(statement instanceof ListTables);
    final ListTables listTables = (ListTables) statement;
    assertThat(listTables.toString(), is("ListTables{showExtended=false}"));
    Assert.assertThat(listTables.getShowExtended(), is(false));
  }

  @Test
  public void shouldReturnListQueriesForShowQueries() {
    final String statementString = "SHOW QUERIES;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    Assert.assertThat(statement, instanceOf(ListQueries.class));
    final ListQueries listQueries = (ListQueries)statement;
    Assert.assertThat(listQueries.getShowExtended(), is(false));
  }

  @Test
  public void testShowProperties() {
    final String simpleQuery = "SHOW PROPERTIES;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();
    Assert.assertTrue(statement instanceof ListProperties);
    final ListProperties listProperties = (ListProperties) statement;
    Assert.assertTrue(listProperties.toString().equalsIgnoreCase("ListProperties{}"));
  }

  @Test
  public void testSetProperties() {
    final String simpleQuery = "set 'auto.offset.reset'='earliest';";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();
    Assert.assertTrue(statement instanceof SetProperty);
    final SetProperty setProperty = (SetProperty) statement;
    assertThat(setProperty.toString(), is("SetProperty{propertyName='auto.offset.reset', propertyValue='earliest'}"));
    Assert.assertTrue(setProperty.getPropertyName().equalsIgnoreCase("auto.offset.reset"));
    Assert.assertTrue(setProperty.getPropertyValue().equalsIgnoreCase("earliest"));
  }

  @Test
  public void testSelectSinkProperties() {
    final String simpleQuery = "create stream s1 with (timestamp='orderid', partitions = 3) as select "
                         + "col1, col2"
                         + " from orders where col2 is null and col3 is not null or (col3*col2 = "
                         + "12);";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();

    Assert.assertTrue(statement instanceof CreateStreamAsSelect);
    final Query query = ((CreateStreamAsSelect) statement).getQuery();
    Assert.assertTrue(query.getWhere().toString().equalsIgnoreCase("Optional[(((ORDERS.COL2 IS NULL) AND (ORDERS.COL3 IS NOT NULL)) OR ((ORDERS.COL3 * ORDERS.COL2) = 12))]"));
  }

  @Test
  public void shouldParseDropStream() {
    final String simpleQuery = "DROP STREAM STREAM1;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();
    assertThat(statement, instanceOf(DropStream.class));
    final DropStream dropStream = (DropStream)  statement;
    assertThat(dropStream.getName().toString().toUpperCase(), equalTo("STREAM1"));
    assertThat(dropStream.getIfExists(), is(false));
  }

  @Test
  public void shouldParseDropTable() {
    final String simpleQuery = "DROP TABLE TABLE1;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();
    assertThat(statement, instanceOf(DropTable.class));
    final DropTable dropTable = (DropTable)  statement;
    assertThat(dropTable.getName().toString().toUpperCase(), equalTo("TABLE1"));
    assertThat(dropTable.getIfExists(), is(false));
  }

  @Test
  public void shouldParseDropStreamIfExists() {
    final String simpleQuery = "DROP STREAM IF EXISTS STREAM1;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();
    assertThat(statement, instanceOf(DropStream.class));
    final DropStream dropStream = (DropStream)  statement;
    assertThat(dropStream.getName().toString().toUpperCase(), equalTo("STREAM1"));
    assertThat(dropStream.getIfExists(), is(true));
  }

  @Test
  public void shouldParseDropTableIfExists() {
    final String simpleQuery = "DROP TABLE IF EXISTS TABLE1;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();
    assertThat(statement, instanceOf(DropTable.class));
    final DropTable dropTable = (DropTable)  statement;
    assertThat(dropTable.getName().toString().toUpperCase(), equalTo("TABLE1"));
    assertThat(dropTable.getIfExists(), is(true));
  }

  @Test
  public void testInsertInto() {
    final String insertIntoString = "INSERT INTO test0 "
        + "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";

    final Statement statement = KsqlParserTestUtil.buildSingleAst(insertIntoString, metaStore)
        .getStatement();


    assertThat(statement, instanceOf(InsertInto.class));
    final InsertInto insertInto = (InsertInto) statement;
    assertThat(insertInto.getTarget().toString(), equalTo("TEST0"));
    final Query query = insertInto.getQuery();
    assertThat( query.getSelect().getSelectItems().size(), equalTo(3));
    assertThat(query.getFrom(), not(nullValue()));
    assertThat(query.getWhere().isPresent(), equalTo(true));
    assertThat(query.getWhere().get(),  instanceOf(ComparisonExpression.class));
    final ComparisonExpression comparisonExpression = (ComparisonExpression)query.getWhere().get();
    assertThat(comparisonExpression.getType().getValue(), equalTo(">"));

  }

  @Test
  public void shouldSetShowDescriptionsForShowStreamsDescriptions() {
    final String statementString = "SHOW STREAMS EXTENDED;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    Assert.assertThat(statement, instanceOf(ListStreams.class));
    final ListStreams listStreams = (ListStreams)statement;
    Assert.assertThat(listStreams.getShowExtended(), is(true));
  }

  @Test
  public void shouldSetShowDescriptionsForShowTablesDescriptions() {
    final String statementString = "SHOW TABLES EXTENDED;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    Assert.assertThat(statement, instanceOf(ListTables.class));
    final ListTables listTables = (ListTables)statement;
    Assert.assertThat(listTables.getShowExtended(), is(true));
  }

  @Test
  public void shouldSetShowDescriptionsForShowQueriesDescriptions() {
    final String statementString = "SHOW QUERIES EXTENDED;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    Assert.assertThat(statement, instanceOf(ListQueries.class));
    final ListQueries listQueries = (ListQueries)statement;
    Assert.assertThat(listQueries.getShowExtended(), is(true));
  }
  
  private void assertQuerySucceeds(final String sql) {
    final Statement statement = KsqlParserTestUtil.buildSingleAst(sql, metaStore).getStatement();
    assertThat(statement, instanceOf(Query.class));
  }

  @Test
  public void shouldSetWithinExpressionWithSingleWithin() {
    final String statementString = "CREATE STREAM foobar as SELECT * from TEST1 JOIN ORDERS WITHIN "
                                   + "10 SECONDS ON TEST1.col1 = ORDERS.ORDERID ;";

    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

    assertThat(statement, instanceOf(CreateStreamAsSelect.class));

    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;

    final Query query = createStreamAsSelect.getQuery();

    assertThat(query.getFrom(), instanceOf(Join.class));

    final Join join = (Join) query.getFrom();

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

    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

    assertThat(statement, instanceOf(CreateStreamAsSelect.class));

    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;

    final Query query = createStreamAsSelect.getQuery();

    assertThat(query.getFrom(), instanceOf(Join.class));

    final Join join = (Join) query.getFrom();

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

    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

    assertThat(statement, instanceOf(CreateStreamAsSelect.class));

    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;

    final Query query = createStreamAsSelect.getQuery();

    assertThat(query.getFrom(), instanceOf(Join.class));

    final Join join = (Join) query.getFrom();

    assertEquals(Join.Type.INNER, join.getType());
  }

  @Test
  public void shouldHaveLeftJoinTypeWhenOuterIsSpecified() {
    final String statementString = "CREATE STREAM foobar as SELECT * from TEST1 LEFT OUTER JOIN "
                                   + "TEST2 ON TEST1.col1 = TEST2.col1;";

    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

    assertThat(statement, instanceOf(CreateStreamAsSelect.class));

    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;

    final Query query = createStreamAsSelect.getQuery();

    assertThat(query.getFrom(), instanceOf(Join.class));

    final Join join = (Join) query.getFrom();

    assertEquals(Join.Type.LEFT, join.getType());
  }

  @Test
  public void shouldHaveLeftJoinType() {
    final String statementString = "CREATE STREAM foobar as SELECT * from TEST1 LEFT JOIN "
                                   + "TEST2 ON TEST1.col1 = TEST2.col1;";

    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

    assertThat(statement, instanceOf(CreateStreamAsSelect.class));

    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;

    final Query query = createStreamAsSelect.getQuery();

    assertThat(query.getFrom(), instanceOf(Join.class));

    final Join join = (Join) query.getFrom();

    assertEquals(Join.Type.LEFT, join.getType());
  }

  @Test
  public void shouldHaveOuterJoinType() {
    final String statementString = "CREATE STREAM foobar as SELECT * from TEST1 FULL JOIN "
                                   + "TEST2 ON TEST1.col1 = TEST2.col1;";

    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

    assertThat(statement, instanceOf(CreateStreamAsSelect.class));

    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;

    final Query query = createStreamAsSelect.getQuery();

    assertThat(query.getFrom(), instanceOf(Join.class));

    final Join join = (Join) query.getFrom();

    assertEquals(Join.Type.OUTER, join.getType());
  }

  @Test
  public void shouldHaveOuterJoinTypeWhenOuterKeywordIsSpecified() {
    final String statementString = "CREATE STREAM foobar as SELECT * from TEST1 FULL OUTER JOIN "
                                   + "TEST2 ON TEST1.col1 = TEST2.col1;";

    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

    assertThat(statement, instanceOf(CreateStreamAsSelect.class));

    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;

    final Query query = createStreamAsSelect.getQuery();

    assertThat(query.getFrom(), instanceOf(Join.class));

    final Join join = (Join) query.getFrom();

    assertEquals(Join.Type.OUTER, join.getType());
  }

  @Test
  public void shouldAddPrefixEvenIfColumnNameIsTheSameAsStream() {
    final String statementString =
        "CREATE STREAM S AS SELECT address FROM address a;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    assertThat(statement, instanceOf(CreateStreamAsSelect.class));
    final Query query = ((CreateStreamAsSelect) statement).getQuery();
    assertThat(query.getSelect().getSelectItems().get(0),
        equalToColumn("A.ADDRESS", "ADDRESS"));
  }

  @Test
  public void shouldNotAddPrefixIfStreamNameIsPrefix() {
    final String statementString =
        "CREATE STREAM S AS SELECT address.orderid FROM address a;";
    KsqlParserTestUtil.buildSingleAst(statementString, metaStore);
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    assertThat(statement, instanceOf(CreateStreamAsSelect.class));
    final Query query = ((CreateStreamAsSelect) statement).getQuery();
    assertThat(query.getSelect().getSelectItems().get(0),
        equalToColumn("ADDRESS.ORDERID", "ORDERID"));
  }

  @Test
  public void shouldPassIfStreamColumnNameWithAliasIsNotAmbiguous() {
    final String statementString =
        "CREATE STREAM S AS SELECT a.address->city FROM address a;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    assertThat(statement, instanceOf(CreateStreamAsSelect.class));
    final Query query = ((CreateStreamAsSelect) statement).getQuery();
    assertThat(query.getSelect().getSelectItems().get(0),
        equalToColumn("FETCH_FIELD_FROM_STRUCT(A.ADDRESS, 'CITY')", "ADDRESS__CITY"));
  }

  @Test
  public void shouldPassIfStreamColumnNameIsNotAmbiguous() {
    final String statementString =
        "CREATE STREAM S AS SELECT address.address->city FROM address a;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    assertThat(statement, instanceOf(CreateStreamAsSelect.class));
    final Query query = ((CreateStreamAsSelect) statement).getQuery();

    final SelectItem item = query.getSelect().getSelectItems().get(0);
    assertThat(item, equalToColumn(
        "FETCH_FIELD_FROM_STRUCT(ADDRESS.ADDRESS, 'CITY')",
        "ADDRESS__CITY"
    ));
  }

  @Test(expected = KsqlException.class)
  public void shouldFailJoinQueryParseIfStreamColumnNameWithNoAliasIsAmbiguous() {
    final String statementString =
        "CREATE STREAM S AS SELECT itemid FROM address a JOIN itemid on a.itemid = itemid.itemid;";
    KsqlParserTestUtil.buildSingleAst(statementString, metaStore);
  }

  @Test
  public void shouldPassJoinQueryParseIfStreamColumnNameWithAliasIsNotAmbiguous() {
    final String statementString =
        "CREATE STREAM S AS SELECT itemid.itemid FROM address a JOIN itemid on a.itemid = itemid.itemid;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    assertThat(statement, instanceOf(CreateStreamAsSelect.class));
    final Query query = ((CreateStreamAsSelect) statement).getQuery();
    assertThat(query.getSelect().getSelectItems().get(0),
        equalToColumn("ITEMID.ITEMID", "ITEMID_ITEMID"));
  }

  @Test
  public void testSelectWithOnlyColumns() {
    expectedException.expect(ParseFailedException.class);
    expectedException.expectMessage("line 1:21: extraneous input ';' expecting {',', 'FROM'}");

    final String simpleQuery = "SELECT ONLY, COLUMNS;";
    KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore);
  }

  @Test
  public void testSelectWithMissingComma() {
    expectedException.expect(ParseFailedException.class);
    expectedException.expectMessage(containsString("line 1:12: extraneous input 'C' expecting"));

    final String simpleQuery = "SELECT A B C FROM address;";
    KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore);
  }

  @Test
  public void testSelectWithMultipleFroms() {
    expectedException.expect(ParseFailedException.class);
    expectedException.expectMessage(containsString("line 1:22: mismatched input ',' expecting"));

    final String simpleQuery = "SELECT * FROM address, itemid;";
    KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore);
  }

  @Test
  public void shouldParseSimpleComment() {
    final String statementString = "--this is a comment.\n"
        + "SHOW STREAMS;";

    final List<PreparedStatement<?>> statements =  KsqlParserTestUtil.buildAst(statementString, metaStore);

    assertThat(statements, hasSize(1));
    assertThat(statements.get(0).getStatement(), is(instanceOf(ListStreams.class)));
  }

  @Test
  public void shouldParseBracketedComment() {
    final String statementString = "/* this is a bracketed comment. */\n"
        + "SHOW STREAMS;"
        + "/*another comment!*/";

    final List<PreparedStatement<?>> statements = KsqlParserTestUtil.buildAst(statementString, metaStore);

    assertThat(statements, hasSize(1));
    assertThat(statements.get(0).getStatement(), is(instanceOf(ListStreams.class)));
  }

  @Test
  public void shouldParseMultiLineWithInlineComments() {
    final String statementString =
        "SHOW -- inline comment\n"
        + "STREAMS;";

    final List<PreparedStatement<?>> statements =  KsqlParserTestUtil.buildAst(statementString, metaStore);

    assertThat(statements, hasSize(1));
    assertThat(statements.get(0).getStatement(), is(instanceOf(ListStreams.class)));
  }

  @Test
  public void shouldParseMultiLineWithInlineBracketedComments() {
    final String statementString =
        "SHOW /* inline\n"
            + "comment */\n"
            + "STREAMS;";

    final List<PreparedStatement<?>> statements =  KsqlParserTestUtil.buildAst(statementString, metaStore);

    assertThat(statements, hasSize(1));
    assertThat(statements.get(0).getStatement(), is(instanceOf(ListStreams.class)));
  }

  @Test
  public void shouldBuildSearchedCaseStatement() {
    // Given:
    final String statementString =
        "CREATE STREAM S AS SELECT CASE WHEN orderunits < 10 THEN 'small' WHEN orderunits < 100 THEN 'medium' ELSE 'large' END FROM orders;";

    // When:
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

    // Then:
    final SearchedCaseExpression searchedCaseExpression = getSearchedCaseExpressionFromCsas(statement);
    assertThat(searchedCaseExpression.getWhenClauses().size(), equalTo(2));
    assertThat(searchedCaseExpression.getWhenClauses().get(0).getOperand().toString(), equalTo("(ORDERS.ORDERUNITS < 10)"));
    assertThat(searchedCaseExpression.getWhenClauses().get(0).getResult().toString(), equalTo("'small'"));
    assertThat(searchedCaseExpression.getWhenClauses().get(1).getOperand().toString(), equalTo("(ORDERS.ORDERUNITS < 100)"));
    assertThat(searchedCaseExpression.getWhenClauses().get(1).getResult().toString(), equalTo("'medium'"));
    assertTrue(searchedCaseExpression.getDefaultValue().isPresent());
    assertThat(searchedCaseExpression.getDefaultValue().get().toString(), equalTo("'large'"));
  }

  @Test
  public void shouldBuildSearchedCaseWithoutDefaultStatement() {
    // Given:
    final String statementString =
        "CREATE STREAM S AS SELECT CASE WHEN orderunits < 10 THEN 'small' WHEN orderunits < 100 THEN 'medium' END FROM orders;";

    // When:
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

    // Then:
    final SearchedCaseExpression searchedCaseExpression = getSearchedCaseExpressionFromCsas(statement);
    assertThat(searchedCaseExpression.getDefaultValue().isPresent(), equalTo(false));
  }

  // https://github.com/confluentinc/ksql/issues/2287
  @Test
  public void shouldThrowHelpfulErrorMessageIfKeyFieldNotQuoted() {
    // Then:
    expectedException.expect(ParseFailedException.class);
    expectedException.expectMessage("mismatched input 'ID'");

    // When:
    KsqlParserTestUtil.buildSingleAst("CREATE STREAM S (ID INT) WITH (KEY=ID);", metaStore);
  }

  private static SearchedCaseExpression getSearchedCaseExpressionFromCsas(final Statement statement) {
    final Query query = ((CreateStreamAsSelect) statement).getQuery();
    final Expression caseExpression = ((SingleColumn) query.getSelect().getSelectItems().get(0)).getExpression();
    return (SearchedCaseExpression) caseExpression;
  }

  private static Matcher<SelectItem> equalToColumn(
      final String expression,
      final String alias) {
    return new TypeSafeMatcher<SelectItem>() {
      @Override
      protected boolean matchesSafely(SelectItem item) {
        if (!(item instanceof SingleColumn)) {
          return false;
        }

        SingleColumn column = (SingleColumn) item;
        return Objects.equals(column.getExpression().toString(), expression)
            && Objects.equals(column.getAlias(), alias);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(
            String.format("Expression: %s, Alias: %s",
                expression,
                alias));
      }
    };
  }

}
