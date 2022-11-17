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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression.Sign;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.DecimalLiteral;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.DoubleLiteral;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.SearchedCaseExpression;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.windows.WindowTimeClause;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlBaseParser.SingleStatementContext;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.StructAll;
import io.confluent.ksql.parser.tree.AlterSystemProperty;
import io.confluent.ksql.parser.tree.CreateConnector;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.DescribeStreams;
import io.confluent.ksql.parser.tree.DescribeTables;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.JoinedSource;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.parser.tree.ListQueries;
import io.confluent.ksql.parser.tree.ListStreams;
import io.confluent.ksql.parser.tree.ListTables;
import io.confluent.ksql.parser.tree.ListTopics;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.RegisterType;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


@SuppressWarnings("OptionalGetWithoutIsPresent")
public class KsqlParserTest {

  private MutableMetaStore metaStore;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(mock(FunctionRegistry.class));
  }

  @Test
  public void testSimpleQuery() {
    final String simpleQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final PreparedStatement<?> statement = KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore);

    assertThat(statement.getMaskedStatementText(), is(simpleQuery));
    assertThat(statement.getStatement(), is(instanceOf(Query.class)));
    final Query query = (Query) statement.getStatement();
    assertThat(query.getSelect().getSelectItems(), hasSize(3));
    assertThat(query.getFrom(), not(nullValue()));
    Assert.assertTrue(query.getWhere().isPresent());
    assertThat(query.getWhere().get(), is(instanceOf(ComparisonExpression.class)));
    final ComparisonExpression comparisonExpression = (ComparisonExpression) query.getWhere().get();
    assertThat(comparisonExpression.getType().getValue(), is(">"));
  }

  @Test
  public void testProjection() {
    final String queryStr = "SELECT col0, col2, col3 FROM test1;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    assertThat(statement, is(instanceOf(Query.class)));
    final Query query = (Query) statement;
    assertThat(query.getSelect().getSelectItems(), hasSize(3));
    Assert.assertTrue(query.getSelect().getSelectItems().get(0) instanceof SingleColumn);
    final SingleColumn column0 = (SingleColumn) query.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias().isPresent(), is(false));
    assertThat(column0.getExpression().toString(), is("COL0"));
  }

  @Test
  public void testProjectionWithArrayMap() {
    final String queryStr = "SELECT col0, col2, col3, col4[0], col5['key1'] FROM test1;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    assertThat(statement, is(instanceOf(Query.class)));
    final Query query = (Query) statement;
    assertThat(query.getSelect().getSelectItems(), hasSize(5));
    assertThat(query.getSelect().getSelectItems().get(0), is(instanceOf(SingleColumn.class)));
    final SingleColumn column0 = (SingleColumn) query.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias().isPresent(), is(false));
    assertThat(column0.getExpression().toString(), is("COL0"));

    final SingleColumn column3 = (SingleColumn) query.getSelect().getSelectItems().get(3);
    final SingleColumn column4 = (SingleColumn) query.getSelect().getSelectItems().get(4);
    assertThat(column3.getExpression().toString(), is("COL4[0]"));
    assertThat(column4.getExpression().toString(), is("COL5['key1']"));
  }

  @Test
  public void testProjectFilter() {
    final String queryStr = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    assertThat(statement, is(instanceOf(Query.class)));
    final Query query = (Query) statement;

    Assert.assertTrue(query.getWhere().get() instanceof ComparisonExpression);
    final ComparisonExpression comparisonExpression = (ComparisonExpression) query.getWhere().get();
    assertThat(comparisonExpression.toString(), is("(COL0 > 100)"));
    assertThat(query.getSelect().getSelectItems(), hasSize(3));
  }

  @Test
  public void testBinaryExpression() {
    final String queryStr = "SELECT col0+10, col2, col3-col1 FROM test1;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    assertThat(statement, is(instanceOf(Query.class)));
    final Query query = (Query) statement;
    final SingleColumn column0 = (SingleColumn) query.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias().isPresent(), is(false));
    assertThat(column0.getExpression().toString(), is("(COL0 + 10)"));
  }

  @Test
  public void testBooleanExpression() {
    final String queryStr = "SELECT col0 = 10, col2, col3 > col1 FROM test1;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    assertThat(statement, is(instanceOf(Query.class)));
    final Query query = (Query) statement;
    final SingleColumn column0 = (SingleColumn) query.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias().isPresent(), is(false));
    assertThat(column0.getExpression().toString(), is("(COL0 = 10)"));
  }

  @Test
  public void testLiterals() {
    final String queryStr = "SELECT 10, col2, 'test', 2.5, true, -5, 2.5e2 FROM test1;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    assertThat(statement, is(instanceOf(Query.class)));
    final Query query = (Query) statement;
    final SingleColumn column0 = (SingleColumn) query.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias().isPresent(), is(false));
    assertThat(column0.getExpression().toString(), is("10"));

    final SingleColumn column1 = (SingleColumn) query.getSelect().getSelectItems().get(1);
    assertThat(column1.getAlias().isPresent(), is(false));
    assertThat(column1.getExpression().toString(), is("COL2"));

    final SingleColumn column2 = (SingleColumn) query.getSelect().getSelectItems().get(2);
    assertThat(column2.getAlias().isPresent(), is(false));
    assertThat(column2.getExpression().toString(), is("'test'"));

    final SingleColumn column3 = (SingleColumn) query.getSelect().getSelectItems().get(3);
    assertThat(column3.getAlias().isPresent(), is(false));
    assertThat(column3.getExpression().toString(), is("2.5"));

    final SingleColumn column4 = (SingleColumn) query.getSelect().getSelectItems().get(4);
    assertThat(column4.getAlias().isPresent(), is(false));
    assertThat(column4.getExpression().toString(), is("true"));

    final SingleColumn column5 = (SingleColumn) query.getSelect().getSelectItems().get(5);
    assertThat(column5.getAlias().isPresent(), is(false));
    assertThat(column5.getExpression().toString(), is("-5"));

    final SingleColumn column6 = (SingleColumn) query.getSelect().getSelectItems().get(6);
    assertThat(column6.getAlias().isPresent(), is(false));
    assertThat(column6.getExpression().toString(), is("2.5E2"));
  }

  private <T, L extends Literal> void shouldParseNumericLiteral(final T value,
                                                                final L expectedValue) {
    final String queryStr = String.format("SELECT " + value.toString() + " FROM test1;", value);
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    assertThat(statement, instanceOf(Query.class));
    final Query query = (Query) statement;
    final SingleColumn column0
        = (SingleColumn) query.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias().isPresent(), is(false));
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
    final String queryStr = "SELECT -12345 FROM test1;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    assertThat(statement, instanceOf(Query.class));
    final Query query = (Query) statement;
    final SingleColumn column0
        = (SingleColumn) query.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias().isPresent(), is(false));
    assertThat(column0.getExpression(), instanceOf(IntegerLiteral.class));
    final IntegerLiteral aue = (IntegerLiteral) column0.getExpression();
    assertThat(aue.getValue(), equalTo(-12345));
  }

  @Test
  public void shouldParseDoubleNegativeInteger() {
    final String queryStr = "SELECT -(-12345) FROM test1;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    assertThat(statement, instanceOf(Query.class));
    final Query query = (Query) statement;
    final SingleColumn column0
        = (SingleColumn) query.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias().isPresent(), is(false));
    assertThat(column0.getExpression(), instanceOf(ArithmeticUnaryExpression.class));
    final ArithmeticUnaryExpression aue = (ArithmeticUnaryExpression) column0.getExpression();
    assertThat(aue.getSign(), equalTo(Sign.MINUS));
    assertThat(((IntegerLiteral) aue.getValue()).getValue(), equalTo(-12345));
  }


  @Test
  public void testBooleanLogicalExpression() {
    final String
        queryStr =
        "SELECT 10, col2, 'test', 2.5, true, -5 FROM test1 WHERE col1 = 10 AND col2 LIKE 'val' OR col4 > 2.6 ;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    assertThat(statement, is(instanceOf(Query.class)));
    final Query query = (Query) statement;
    final SingleColumn column0 = (SingleColumn) query.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias().isPresent(), is(false));
    assertThat(column0.getExpression().toString(), is("10"));

    final SingleColumn column1 = (SingleColumn) query.getSelect().getSelectItems().get(1);
    assertThat(column1.getAlias().isPresent(), is(false));
    assertThat(column1.getExpression().toString(), is("COL2"));

    final SingleColumn column2 = (SingleColumn) query.getSelect().getSelectItems().get(2);
    assertThat(column2.getAlias().isPresent(), is(false));
    assertThat(column2.getExpression().toString(), is("'test'"));
  }

  @Test
  public void shouldParseStructFieldAccessCorrectly() {
    final String simpleQuery = "SELECT iteminfo->category->name, address->street FROM orders WHERE address->state = 'CA';";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();


    assertThat(statement, is(instanceOf(Query.class)));
    final Query query = (Query) statement;
    assertThat(query.getSelect().getSelectItems(), hasSize(2));
    final SingleColumn singleColumn0 = (SingleColumn) query.getSelect().getSelectItems().get(0);
    final SingleColumn singleColumn1 = (SingleColumn) query.getSelect().getSelectItems().get(1);
    assertThat(singleColumn0.getExpression(), instanceOf(DereferenceExpression.class));
    assertThat(singleColumn0.getExpression().toString(), is("ITEMINFO->CATEGORY->NAME"));
    assertThat(singleColumn1.getExpression().toString(), is("ADDRESS->STREET"));
  }

  @Test
  public void shouldParseStartOnStructCorrectly() {
    final String simpleQuery = "SELECT iteminfo->* FROM orders WHERE address->state = 'CA';";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();

    assertThat(statement, is(instanceOf(Query.class)));
    final Query query = (Query) statement;
    assertThat(query.getSelect().getSelectItems(), hasSize(1));
    final StructAll allFields = (StructAll) query.getSelect().getSelectItems().get(0);
    assertThat(allFields.getBaseStruct(), instanceOf(Expression.class));
    assertThat(allFields.getBaseStruct().toString(), is("ITEMINFO"));
  }

  @Test
  public void testSimpleLeftJoin() {
    final String
        queryStr =
        "SELECT t1.col1, t2.col1, t2.col4, col5, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON "
            + "t1.col1 = t2.col1;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    assertThat(statement, is(instanceOf(Query.class)));
    final Query query = (Query) statement;
    assertThat(query.getFrom(), is(instanceOf(Join.class)));
    final Join join = (Join) query.getFrom();
    assertThat(Iterables.getOnlyElement(join.getRights()).getType().toString(), is("LEFT"));
    assertThat(((AliasedRelation) join.getLeft()).getAlias(), is(SourceName.of("T1")));
    assertThat(((AliasedRelation) Iterables.getOnlyElement(join.getRights()).getRelation()).getAlias(), is(SourceName.of("T2")));
  }

  @Test
  public void testLeftJoinWithFilter() {
    final String
        queryStr =
        "SELECT t1.col1, t2.col1, t2.col4, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON t1.col1 = "
            + "t2.col1 WHERE t2.col2 = 'test';";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    assertThat(statement, is(instanceOf(Query.class)));
    final Query query = (Query) statement;
    assertThat(query.getFrom(), is(instanceOf(Join.class)));
    final Join join = (Join) query.getFrom();
    assertThat(Iterables.getOnlyElement(join.getRights()).getType().toString(), is("LEFT"));
    assertThat(((AliasedRelation) join.getLeft()).getAlias(), is(SourceName.of("T1")));
    assertThat(((AliasedRelation) Iterables.getOnlyElement(join.getRights()).getRelation()).getAlias(), is(SourceName.of("T2")));
    assertThat(query.getWhere().get().toString(), is("(T2.COL2 = 'test')"));
  }

  @Test
  public void testSimpleRightJoin() {
    final String
        queryStr =
        "SELECT t1.col1, t2.col1, t2.col4, col5, t2.col2 FROM test1 t1 RIGHT JOIN test2 t2 ON "
            + "t1.col1 = t2.col1;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    assertThat(statement, is(instanceOf(Query.class)));
    final Query query = (Query) statement;
    assertThat(query.getFrom(), is(instanceOf(Join.class)));
    final Join join = (Join) query.getFrom();
    assertThat(Iterables.getOnlyElement(join.getRights()).getType().toString(), is("RIGHT"));
    assertThat(((AliasedRelation) join.getLeft()).getAlias(), is(SourceName.of("T1")));
    assertThat(((AliasedRelation) Iterables.getOnlyElement(join.getRights()).getRelation()).getAlias(), is(SourceName.of("T2")));
  }

  @Test
  public void testRightJoinWithFilter() {
    final String
        queryStr =
        "SELECT t1.col1, t2.col1, t2.col4, t2.col2 FROM test1 t1 RIGHT JOIN test2 t2 ON t1.col1 = "
            + "t2.col1 WHERE t2.col2 = 'test';";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    assertThat(statement, is(instanceOf(Query.class)));
    final Query query = (Query) statement;
    assertThat(query.getFrom(), is(instanceOf(Join.class)));
    final Join join = (Join) query.getFrom();
    assertThat(Iterables.getOnlyElement(join.getRights()).getType().toString(), is("RIGHT"));
    assertThat(((AliasedRelation) join.getLeft()).getAlias(), is(SourceName.of("T1")));
    assertThat(((AliasedRelation) Iterables.getOnlyElement(join.getRights()).getRelation()).getAlias(), is(SourceName.of("T2")));
    assertThat(query.getWhere().get().toString(), is("(T2.COL2 = 'test')"));
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
  public void testCreateSourceTable() {
    // When:
    final CreateTable stmt = (CreateTable) KsqlParserTestUtil.buildSingleAst(
        "CREATE SOURCE TABLE foozball (id VARCHAR PRIMARY KEY) WITH (kafka_topic='foozball', " +
            "value_format='json', partitions=1, replicas=-1);", metaStore).getStatement();

    // Then:
    assertThat(stmt.isSource(), is(true));
  }

  @Test
  public void testCreateSourceStream() {
    // When:
    final CreateStream stmt = (CreateStream) KsqlParserTestUtil.buildSingleAst(
        "CREATE SOURCE STREAM foozball (id VARCHAR KEY) WITH (kafka_topic='foozball', " +
            "value_format='json', partitions=1, replicas=-1);", metaStore).getStatement();

    // Then:
    assertThat(stmt.isSource(), is(true));
  }

  @Test
  public void testNegativeInWith() {
    // When:
    final CreateSource stmt = (CreateSource) KsqlParserTestUtil.buildSingleAst(
        "CREATE STREAM foozball (id VARCHAR) WITH (kafka_topic='foozball', value_format='json', partitions=1, replicas=-1);",
        metaStore).getStatement();

    // Then:
    assertThat(stmt.getProperties().getReplicas(), is(Optional.of((short) -1)));
  }

  @Test
  public void shouldThrowOnNonAlphanumericSourceName() {
    // When:
    final ParseFailedException e = assertThrows(
        ParseFailedException.class,
        () -> KsqlParserTestUtil.buildSingleAst(
            "CREATE STREAM `foo!bar` WITH (kafka_topic='foo', value_format='AVRO');",
            metaStore)
    );

    // Then:
    assertThat(e.getUnloggedMessage(), containsString(
        "Got: 'foo!bar'"));
    assertThat(e.getMessage(), containsString("Illegal argument at Line: 1, Col: 15. Source names may only contain alphanumeric values, '_' or '-'."));
    assertThat(e.getSqlStatement(), containsString("foo!bar"));
  }

  @Test
  public void shouldAllowEscapedTerminateQuery() {
    // When:
    final PreparedStatement<TerminateQuery> statement = KsqlParserTestUtil
        .buildSingleAst("TERMINATE `CSAS-foo_2`;", metaStore);

    // Then:
    assertThat(statement.getStatement().getQueryId().map(QueryId::toString), is(Optional.of("CSAS-foo_2")));
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
    assertThat(((AliasedRelation) join.getLeft()).getAlias(), is(SourceName.of("T1")));
    assertThat(((AliasedRelation) Iterables.getOnlyElement(join.getRights()).getRelation()).getAlias(), is(SourceName.of("T2")));
  }

  @Test
  public void testUDF() {
    final String queryStr = "SELECT lcase(col1), concat(col2,'hello'), floor(abs(col3)) FROM test1 t1;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    Assert.assertTrue("testSelectAll fails", statement instanceof Query);
    final Query query = (Query) statement;

    final SingleColumn column0 = (SingleColumn) query.getSelect().getSelectItems().get(0);
    assertThat(column0.getAlias().isPresent(), is(false));
    assertThat(column0.getExpression().toString(), is("LCASE(COL1)"));

    final SingleColumn column1 = (SingleColumn) query.getSelect().getSelectItems().get(1);
    assertThat(column1.getAlias().isPresent(), is(false));
    assertThat(column1.getExpression().toString(), is("CONCAT(COL2, 'hello')"));

    final SingleColumn column2 = (SingleColumn) query.getSelect().getSelectItems().get(2);
    assertThat(column2.getAlias().isPresent(), is(false));
    assertThat(column2.getExpression().toString(), is("FLOOR(ABS(COL3))"));
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
        + ") WITH (value_format = 'avro', kafka_topic='orders_topic', sourced_by_connector='jdbc');", metaStore).getStatement();

    // Then:
    assertThat(result.getName(), equalTo(SourceName.of("ORDERS")));
    assertThat(Iterables.size(result.getElements()), equalTo(7));
    assertThat(Iterables.get(result.getElements(), 0).getName(), equalTo(ColumnName.of("ORDERTIME")));
    assertThat(Iterables.get(result.getElements(), 6).getType().getSqlType().baseType(), equalTo(SqlBaseType.STRUCT));
    assertThat(result.getProperties().getKafkaTopic(), equalTo("orders_topic"));
    assertThat(result.getProperties().getValueFormat().map(FormatInfo::getFormat), equalTo(Optional.of("AVRO")));
    assertThat(result.getProperties().getSourceConnector(), equalTo(Optional.of("jdbc")));
  }

  @Test
  public void testCreateTable() {
    // When:
    final CreateTable result = (CreateTable) KsqlParserTestUtil.buildSingleAst(
        "CREATE TABLE users (usertime bigint, userid varchar PRIMARY KEY, regionid varchar, gender varchar) "
            + "WITH (kafka_topic='foo', value_format='json', sourced_by_connector='jdbc');", metaStore).getStatement();

    // Then:
    assertThat(result.getName(), equalTo(SourceName.of("USERS")));
    assertThat(Iterables.size(result.getElements()), equalTo(4));
    assertThat(Iterables.get(result.getElements(), 0).getName(), equalTo(ColumnName.of("USERTIME")));
    assertThat(result.getProperties().getKafkaTopic(), equalTo("foo"));
    assertThat(result.getProperties().getValueFormat().map(FormatInfo::getFormat), equalTo(Optional.of("JSON")));
    assertThat(result.getProperties().getSourceConnector(), equalTo(Optional.of("jdbc")));
  }

  @Test
  public void testPrintTopicNameLowerCase() {
    // When:
    final PrintTopic result = (PrintTopic) KsqlParserTestUtil.buildSingleAst(
        "PRINT topic_name_in_lower_case;", metaStore).getStatement();

    // Then:
    assertThat(result.getTopic(), equalTo("topic_name_in_lower_case"));
  }

  @Test
  public void testPrintTopicNameUpperCase() {
    // When:
    final PrintTopic result = (PrintTopic) KsqlParserTestUtil.buildSingleAst(
        "PRINT TOPIC_NAME_IN_UPPER_CASE;", metaStore).getStatement();

    // Then:
    assertThat(result.getTopic(), equalTo("TOPIC_NAME_IN_UPPER_CASE"));
  }

  @Test
  public void testPrintTopicNameQuoted() {
    // When:
    final PrintTopic result = (PrintTopic) KsqlParserTestUtil.buildSingleAst(
        "PRINT 'topic_name_in_lower_case';", metaStore).getStatement();

    // Then:
    assertThat(result.getTopic(), equalTo("topic_name_in_lower_case"));
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
    assertThat(csas.getName(), equalTo(SourceName.of("BIGORDERS_JSON")));
    final Query query = csas.getQuery();
    assertThat(query.getSelect().getSelectItems(), is(contains(new AllColumns(Optional.empty()))));
    assertThat(query.getWhere().get().toString().toUpperCase(), equalTo("(ORDERUNITS > 5)"));
    assertThat(((AliasedRelation) query.getFrom()).getAlias().text().toUpperCase(), equalTo("ORDERS"));
  }

  @Test
  public void shouldFailIfSourcedByConnectorProvidedCXAS() {
    for (final String source: ImmutableSet.of("STREAM", "TABLE")) {
      // Given:
      final String cxasQuery =
          "CREATE " + source
          + " bigorders_json WITH (value_format = 'json', "
          + "kafka_topic='bigorders_topic', sourced_by_connector='jdbc') AS SELECT * FROM orders;";

      // When:
      final Exception e = assertThrows(
          KsqlException.class,
          () -> KsqlParserTestUtil.<CreateStreamAsSelect>buildSingleAst(
              cxasQuery,
              metaStore)
      );

      // Then:
      assertThat(e.getMessage(), containsString("Failed to prepare statement: " +
          "Invalid config variable(s) in the WITH clause: SOURCED_BY_CONNECTOR"));
    }
  }

  @Test
  public void testShouldFailIfWrongKeyword() {
    try {
      final String simpleQuery = "SELLECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
      KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore);
      fail(format("Expected query: %s to fail", simpleQuery));
    } catch (final ParseFailedException e) {
      assertThat(
          e.getUnloggedMessage(),
          containsString("line 1:1: Syntax Error\n" +
              "Unknown statement 'SELLECT'\n" +
              "Did you mean 'SELECT'?")
      );
      assertThat(
          e.getMessage(),
          containsString("line 1:1: Syntax error at line 1:1")
      );
      assertThat(
          e.getSqlStatement(),
          containsString("SELLECT col0, col2, col3 FROM test1 WHERE col0 > 100")
      );
    }
  }

  @Test
  public void testSelectTumblingWindow() {

    final String
        queryStr =
        "select itemid, sum(orderunits) from orders window TUMBLING ( size 30 second) where orderunits > 5 group by itemid;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(queryStr, metaStore).getStatement();
    assertThat(statement, is(instanceOf(Query.class)));
    final Query query = (Query) statement;
    assertThat(query.getSelect().getSelectItems(), hasSize(2));
    assertThat(query.getWhere().get().toString(), is("(ORDERUNITS > 5)"));
    assertThat(((AliasedRelation) query.getFrom()).getAlias(), is(SourceName.of("ORDERS")));
    Assert.assertTrue(query.getWindow().isPresent());
    assertThat(query.getWindow().get().toString(), is(" WINDOW STREAMWINDOW  TUMBLING ( SIZE 30 SECONDS ) "));
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
    assertThat(query.getSelect().getSelectItems(), hasSize(2));
    assertThat(query.getWhere().get().toString(), equalTo("(ORDERUNITS > 5)"));
    assertThat(((AliasedRelation) query.getFrom()).getAlias().text().toUpperCase(), equalTo("ORDERS"));
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
    assertThat(statement, is(instanceOf(Query.class)));
    final Query query = (Query) statement;
    assertThat(query.getSelect().getSelectItems(), hasSize(2));
    assertThat(query.getWhere().get().toString(), is("(ORDERUNITS > 5)"));
    assertThat(((AliasedRelation) query.getFrom()).getAlias(), is(SourceName.of("ORDERS")));
    Assert.assertTrue(query
        .getWindow().isPresent());
    assertThat(query
        .getWindow().get().toString(), is(" WINDOW STREAMWINDOW  SESSION "
        + "( 30 SECONDS ) "));
  }

  @Test
  public void testShowTopics() {
    // Given:
    final String simpleQuery = "SHOW TOPICS;";

    // When:
    final Statement statement = KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();

    // Then:
    Assert.assertTrue(statement instanceof ListTopics);
    final ListTopics listTopics = (ListTopics) statement;
    assertThat(listTopics.toString(), is("ListTopics{showAll=false, showExtended=false}"));
    assertThat(listTopics.getShowExtended(), is(false));
  }

  @Test
  public void testShowStreams() {
    final String simpleQuery = "SHOW STREAMS;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();
    Assert.assertTrue(statement instanceof ListStreams);
    final ListStreams listStreams = (ListStreams) statement;
    assertThat(listStreams.toString(), is("ListStreams{showExtended=false}"));
    assertThat(listStreams.getShowExtended(), is(false));
  }

  @Test
  public void testShowTables() {
    final String simpleQuery = "SHOW TABLES;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();
    Assert.assertTrue(statement instanceof ListTables);
    final ListTables listTables = (ListTables) statement;
    assertThat(listTables.toString(), is("ListTables{showExtended=false}"));
    assertThat(listTables.getShowExtended(), is(false));
  }

  @Test
  public void shouldReturnListQueriesForShowQueries() {
    final String statementString = "SHOW QUERIES;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    assertThat(statement, instanceOf(ListQueries.class));
    final ListQueries listQueries = (ListQueries) statement;
    assertThat(listQueries.getShowExtended(), is(false));
  }

  @Test
  public void testShowProperties() {
    final String simpleQuery = "SHOW PROPERTIES;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();
    Assert.assertTrue(statement instanceof ListProperties);
    final ListProperties listProperties = (ListProperties) statement;
    assertThat(listProperties.toString(), is("ListProperties{}"));
  }

  @Test
  public void testSetProperties() {
    final String simpleQuery = "set 'auto.offset.reset'='earliest';";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();
    Assert.assertTrue(statement instanceof SetProperty);
    final SetProperty setProperty = (SetProperty) statement;
    assertThat(setProperty.toString(), is("SetProperty{propertyName='auto.offset.reset', propertyValue='earliest'}"));
    assertThat(setProperty.getPropertyName(), is("auto.offset.reset"));
    assertThat(setProperty.getPropertyValue(), is("earliest"));
  }

  @Test
  public void testAlterSystemProperties() {
    final String simpleQuery = "ALTER SYSTEM 'ksql.persistent.prefix'='test';";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();
    Assert.assertTrue(statement instanceof AlterSystemProperty);
    final AlterSystemProperty alterSystemProperty = (AlterSystemProperty) statement;
    assertThat(alterSystemProperty.toString(), is("AlterSystemProperty{propertyName='ksql.persistent.prefix', propertyValue='test'}"));
    assertThat(alterSystemProperty.getPropertyName(), is("ksql.persistent.prefix"));
    assertThat(alterSystemProperty.getPropertyValue(), is("test"));
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
    assertThat(query.getWhere().toString(), is("Optional[(((COL2 IS NULL) AND (COL3 IS NOT NULL)) OR ((COL3 * COL2) = 12))]"));
  }

  @Test
  public void shouldParseDropStream() {
    final String simpleQuery = "DROP STREAM STREAM1;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();
    assertThat(statement, instanceOf(DropStream.class));
    final DropStream dropStream = (DropStream) statement;
    assertThat(dropStream.getName(), equalTo(SourceName.of("STREAM1")));
    assertThat(dropStream.getIfExists(), is(false));
  }

  @Test
  public void shouldParseDropTable() {
    final String simpleQuery = "DROP TABLE TABLE1;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();
    assertThat(statement, instanceOf(DropTable.class));
    final DropTable dropTable = (DropTable) statement;
    assertThat(dropTable.getName(), equalTo(SourceName.of("TABLE1")));
    assertThat(dropTable.getIfExists(), is(false));
  }

  @Test
  public void shouldParseDropStreamIfExists() {
    final String simpleQuery = "DROP STREAM IF EXISTS STREAM1;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();
    assertThat(statement, instanceOf(DropStream.class));
    final DropStream dropStream = (DropStream) statement;
    assertThat(dropStream.getName(), equalTo(SourceName.of("STREAM1")));
    assertThat(dropStream.getIfExists(), is(true));
  }

  @Test
  public void shouldParseDropTableIfExists() {
    final String simpleQuery = "DROP TABLE IF EXISTS TABLE1;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore).getStatement();
    assertThat(statement, instanceOf(DropTable.class));
    final DropTable dropTable = (DropTable) statement;
    assertThat(dropTable.getName(), equalTo(SourceName.of("TABLE1")));
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
    assertThat(insertInto.getTarget(), equalTo(SourceName.of("TEST0")));
    final Query query = insertInto.getQuery();
    assertThat(query.getSelect().getSelectItems(), hasSize(3));
    assertThat(query.getFrom(), not(nullValue()));
    assertThat(query.getWhere().isPresent(), equalTo(true));
    assertThat(query.getWhere().get(), instanceOf(ComparisonExpression.class));
    final ComparisonExpression comparisonExpression = (ComparisonExpression) query.getWhere().get();
    assertThat(comparisonExpression.getType().getValue(), equalTo(">"));

  }

  @Test
  public void shouldSetShowDescriptionsForShowStreamsDescriptions() {
    final String statementString = "SHOW STREAMS EXTENDED;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    assertThat(statement, instanceOf(ListStreams.class));
    final ListStreams listStreams = (ListStreams) statement;
    assertThat(listStreams.getShowExtended(), is(true));
  }

  @Test
  public void shouldSetShowDescriptionsForShowTopicsDescriptions() {
    // Given:
    final String statementString = "SHOW TOPICS EXTENDED;";

    // When:
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

    // Then:
    assertThat(statement, instanceOf(ListTopics.class));
    final ListTopics listTopics = (ListTopics) statement;
    assertThat(listTopics.getShowExtended(), is(true));
  }

  @Test
  public void shouldSetShowDescriptionsForShowTablesDescriptions() {
    final String statementString = "SHOW TABLES EXTENDED;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    assertThat(statement, instanceOf(ListTables.class));
    final ListTables listTables = (ListTables) statement;
    assertThat(listTables.getShowExtended(), is(true));
  }

  @Test
  public void shouldSetShowDescriptionsForShowQueriesDescriptions() {
    final String statementString = "SHOW QUERIES EXTENDED;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    assertThat(statement, instanceOf(ListQueries.class));
    final ListQueries listQueries = (ListQueries) statement;
    assertThat(listQueries.getShowExtended(), is(true));
  }

  @Test
  public void testDescribeStreams() {
    final String statementString = "DESCRIBE STREAMS;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    assertThat(statement, instanceOf(DescribeStreams.class));
    final DescribeStreams describeStreams = (DescribeStreams) statement;
    assertThat(describeStreams.getShowExtended(), is(false));
  }

  @Test
  public void testDescribeTables() {
    final String statementString = "DESCRIBE TABLES;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    assertThat(statement, instanceOf(DescribeTables.class));
    final DescribeTables describeTables = (DescribeTables) statement;
    assertThat(describeTables.getShowExtended(), is(false));
  }

  @Test
  public void testDescribeStreamsExtended() {
    final String statementString = "DESCRIBE STREAMS EXTENDED;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    assertThat(statement, instanceOf(DescribeStreams.class));
    final DescribeStreams describeStreams = (DescribeStreams) statement;
    assertThat(describeStreams.getShowExtended(), is(true));
  }

  @Test
  public void testDescribeTablesExtended() {
    final String statementString = "DESCRIBE TABLES EXTENDED;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    assertThat(statement, instanceOf(DescribeTables.class));
    final DescribeTables describeTables = (DescribeTables) statement;
    assertThat(describeTables.getShowExtended(), is(true));
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

    final JoinedSource join = Iterables.getOnlyElement(((Join) query.getFrom()).getRights());

    assertTrue(join.getWithinExpression().isPresent());

    final WithinExpression withinExpression = join.getWithinExpression().get();

    assertThat(withinExpression.getBefore(), is(10L));
    assertThat(withinExpression.getAfter(), is(10L));
    assertThat(withinExpression.getBeforeTimeUnit(), is(TimeUnit.SECONDS));
    assertThat(withinExpression.getGrace(), is(Optional.empty()));
    assertThat(join.getType(), is(JoinedSource.Type.INNER));
  }

  @Test
  public void shouldSetWithinExpressionWithSingleWithinAndGracePeriod() {
    final String statementString = "CREATE STREAM foobar as SELECT * from TEST1 JOIN ORDERS WITHIN "
        + "10 SECONDS GRACE PERIOD 5 SECONDS ON TEST1.col1 = ORDERS.ORDERID ;";

    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

    assertThat(statement, instanceOf(CreateStreamAsSelect.class));

    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;

    final Query query = createStreamAsSelect.getQuery();

    assertThat(query.getFrom(), instanceOf(Join.class));

    final JoinedSource join = Iterables.getOnlyElement(((Join) query.getFrom()).getRights());

    assertTrue(join.getWithinExpression().isPresent());

    final WithinExpression withinExpression = join.getWithinExpression().get();

    assertThat(withinExpression.getBefore(), is(10L));
    assertThat(withinExpression.getAfter(), is(10L));
    assertThat(withinExpression.getBeforeTimeUnit(), is(TimeUnit.SECONDS));
    assertThat(withinExpression.getGrace(),
        is(Optional.of(new WindowTimeClause(5, TimeUnit.SECONDS))));
    assertThat(join.getType(), is(JoinedSource.Type.INNER));
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

    final JoinedSource join = Iterables.getOnlyElement(((Join) query.getFrom()).getRights());

    assertTrue(join.getWithinExpression().isPresent());

    final WithinExpression withinExpression = join.getWithinExpression().get();

    assertThat(withinExpression.getBefore(), is(10L));
    assertThat(withinExpression.getAfter(), is(20L));
    assertThat(withinExpression.getBeforeTimeUnit(), is(TimeUnit.SECONDS));
    assertThat(withinExpression.getAfterTimeUnit(), is(TimeUnit.MINUTES));
    assertThat(withinExpression.getGrace(), is(Optional.empty()));
    assertThat(join.getType(), is(JoinedSource.Type.INNER));
  }

  @Test
  public void shouldSetWithinExpressionWithBeforeAndAfterAndGracePeriod() {
    final String statementString = "CREATE STREAM foobar as SELECT * from TEST1 JOIN ORDERS "
        + "WITHIN (10 seconds, 20 minutes) GRACE PERIOD 10 minutes "
        + "ON TEST1.col1 = ORDERS.ORDERID ;";

    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

    assertThat(statement, instanceOf(CreateStreamAsSelect.class));

    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statement;

    final Query query = createStreamAsSelect.getQuery();

    assertThat(query.getFrom(), instanceOf(Join.class));

    final JoinedSource join = Iterables.getOnlyElement(((Join) query.getFrom()).getRights());

    assertTrue(join.getWithinExpression().isPresent());

    final WithinExpression withinExpression = join.getWithinExpression().get();

    assertThat(withinExpression.getBefore(), is(10L));
    assertThat(withinExpression.getAfter(), is(20L));
    assertThat(withinExpression.getBeforeTimeUnit(), is(TimeUnit.SECONDS));
    assertThat(withinExpression.getAfterTimeUnit(), is(TimeUnit.MINUTES));
    assertThat(withinExpression.getGrace(),
        is(Optional.of(new WindowTimeClause(10, TimeUnit.MINUTES))));
    assertThat(join.getType(), is(JoinedSource.Type.INNER));
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

    final JoinedSource join = Iterables.getOnlyElement(((Join) query.getFrom()).getRights());

    assertThat(join.getType(), is(JoinedSource.Type.INNER));
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

    final JoinedSource join = Iterables.getOnlyElement(((Join) query.getFrom()).getRights());

    assertThat(join.getType(), is(JoinedSource.Type.LEFT));
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

    final JoinedSource join = Iterables.getOnlyElement(((Join) query.getFrom()).getRights());

    assertThat(join.getType(), is(JoinedSource.Type.LEFT));
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

    final JoinedSource join = Iterables.getOnlyElement(((Join) query.getFrom()).getRights());

    assertThat(join.getType(), is(JoinedSource.Type.OUTER));
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

    final JoinedSource join = Iterables.getOnlyElement(((Join) query.getFrom()).getRights());

    assertThat(join.getType(), is(JoinedSource.Type.OUTER));
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
        equalToColumn("ADDRESS"));
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
        equalToColumn("ADDRESS.ORDERID"));
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
        equalToColumn("A.ADDRESS->CITY"));
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
    assertThat(item, equalToColumn("ADDRESS.ADDRESS->CITY"));
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
        equalToColumn("ITEMID.ITEMID", Optional.empty()));
  }

  @Test
  public void testSelectWithOnlyColumns() {
    // Given:
    final String simpleQuery = "SELECT ONLY, COLUMNS;";

    // When:
    final ParseFailedException e = assertThrows(
        ParseFailedException.class,
        () -> KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore)
    );

    // Then:
    assertThat(e.getUnloggedMessage(), containsString("line 1:21: Syntax Error\n" +
        "Expecting {',', 'FROM'}"));
    assertThat(e.getMessage(), containsString("line 1:21: Syntax error at line 1:21"));
    assertThat(e.getSqlStatement(), containsString("SELECT ONLY, COLUMNS"));
  }

  @Test
  public void testSelectWithMissingComma() {
    // Given:
    final String simpleQuery = "SELECT A B C FROM address;";

    // When:
    final ParseFailedException e = assertThrows(
        ParseFailedException.class,
        () -> KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore)
    );

    // Then:
    assertThat(e.getUnloggedMessage(), containsString("line 1:12: Syntax Error"));
    assertThat(e.getMessage(), containsString("line 1:12: Syntax error at line 1:12"));
    assertThat(e.getSqlStatement(), containsString("SELECT A B C FROM address"));
  }

  @Test
  public void testReservedKeywordSyntaxError() {
    // Given:
    final String simpleQuery = "CREATE STREAM ORDERS (c1 VARCHAR, size INTEGER)\n" +
            "  WITH (kafka_topic='ords', value_format='json');";

    // When:
    final ParseFailedException e = assertThrows(
            ParseFailedException.class,
            () -> KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore)
    );

    // Then:
    assertThat(e.getUnloggedMessage(), containsString(
        "line 1:35: Syntax Error\n"
            + "\"size\" is a reserved keyword and it can't be used as an identifier."
            + " You can use it as an identifier by escaping it as 'size' "
    ));
    assertThat(e.getMessage(), containsString("line 1:35: Syntax error at line 1:35"));
    assertThat(e.getSqlStatement(), containsString(
        "CREATE STREAM ORDERS (c1 VARCHAR, size INTEGER)"
    ));
  }

  @Test
  public void testReservedKeywordSyntaxErrorCaseInsensitive() {
    // Given:
    final String simpleQuery = "CREATE STREAM ORDERS (Load VARCHAR, size INTEGER)\n" +
            "  WITH (kafka_topic='ords', value_format='json');";

    // When:
    final ParseFailedException e = assertThrows(
            ParseFailedException.class,
            () -> KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore)
    );

    // Then:
    assertThat(e.getUnloggedMessage(), containsString("line 1:23: Syntax Error\n" +
        "\"Load\" is a reserved keyword and it can't be used as an identifier. " +
        "You can use it as an identifier by escaping it as 'Load'"));
    assertThat(e.getMessage(), containsString("line 1:23: Syntax error at line 1:23"));
    assertThat(e.getSqlStatement(), containsString("CREATE STREAM ORDERS (Load VARCHAR, size INTEGER)"));
  }

  @Test
  public void testNonReservedKeywordShouldNotThrowException() {
    // 'sink' is a keyword but is non-reserved. should not throw an exception
    final CreateStream result = (CreateStream) KsqlParserTestUtil.buildSingleAst("CREATE STREAM ORDERS" +
            " (place VARCHAR, Sink INTEGER)\n" +
            " WITH (kafka_topic='orders_topic', value_format='json');", metaStore).getStatement();

    // Then:
    assertThat(result.getName(), equalTo(SourceName.of("ORDERS")));
    assertThat(result.getProperties().getKafkaTopic(), equalTo("orders_topic"));
  }

  @Test
  public void testSelectWithMultipleFroms() {
    // Given:
    final String simpleQuery = "SELECT * FROM address, itemid;";

    // When:
    final ParseFailedException e = assertThrows(
        ParseFailedException.class,
        () -> KsqlParserTestUtil.buildSingleAst(simpleQuery, metaStore)
    );

    // Then:
    assertThat(e.getUnloggedMessage(), containsString("line 1:22: Syntax Error"));
    assertThat(e.getMessage(), containsString("line 1:22: Syntax error at line 1:22"));
    assertThat(e.getSqlStatement(), containsString("SELECT * FROM address, itemid;"));
  }

  @Test
  public void shouldParseSimpleComment() {
    final String statementString = "--this is a comment.\n"
        + "SHOW STREAMS;";

    final List<PreparedStatement<?>> statements = KsqlParserTestUtil.buildAst(statementString, metaStore);

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

    final List<PreparedStatement<?>> statements = KsqlParserTestUtil.buildAst(statementString, metaStore);

    assertThat(statements, hasSize(1));
    assertThat(statements.get(0).getStatement(), is(instanceOf(ListStreams.class)));
  }

  @Test
  public void shouldParseMultiLineWithInlineBracketedComments() {
    final String statementString =
        "SHOW /* inline\n"
            + "comment */\n"
            + "STREAMS;";

    final List<PreparedStatement<?>> statements = KsqlParserTestUtil.buildAst(statementString, metaStore);

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
    assertThat(searchedCaseExpression.getWhenClauses().get(0).getOperand().toString(), equalTo("(ORDERUNITS < 10)"));
    assertThat(searchedCaseExpression.getWhenClauses().get(0).getResult().toString(), equalTo("'small'"));
    assertThat(searchedCaseExpression.getWhenClauses().get(1).getOperand().toString(), equalTo("(ORDERUNITS < 100)"));
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

  @Test
  public void shouldBuildCreateSourceConnectorStatement() {
    // When:
    final PreparedStatement<CreateConnector> createExternal =
        KsqlParserTestUtil.buildSingleAst(
            "CREATE SOURCE CONNECTOR foo WITH ('foo.bar'='foo');", metaStore);

    // Then:
    assertThat(createExternal.getStatement().getConfig(), hasEntry("foo.bar", new StringLiteral("foo")));
    assertThat(createExternal.getStatement().getName(), is("FOO"));
    assertThat(createExternal.getStatement().getType(), is(CreateConnector.Type.SOURCE));
  }

  @Test
  public void shouldBuildCreateSinkConnectorStatement() {
    // When:
    final PreparedStatement<CreateConnector> createExternal =
        KsqlParserTestUtil.buildSingleAst(
            "CREATE SINK CONNECTOR foo WITH (\"foo.bar\"='foo');", metaStore);

    // Then:
    assertThat(createExternal.getStatement().getConfig(), hasEntry("foo.bar", new StringLiteral("foo")));
    assertThat(createExternal.getStatement().getName(), is("FOO"));
    assertThat(createExternal.getStatement().getType(), is(CreateConnector.Type.SINK));
  }

  @Test
  public void shouldParseCustomTypesInCreateType() {
    // Given:
    final SqlStruct cookie = SqlStruct.builder().field("type", SqlTypes.STRING).build();
    metaStore.registerType("cookie", cookie);

    // When:
    final PreparedStatement<RegisterType> registerType = KsqlParserTestUtil.buildSingleAst(
        "CREATE TYPE pantry AS ARRAY<COOKIE>;",
        metaStore
    );

    // Then:
    assertThat(registerType.getStatement().getType().getSqlType(), is(SqlArray.of(cookie)));
  }

  @Test
  public void shouldParseCustomTypesInCreateSource() {
    // Given:
    final SqlStruct cookie = SqlStruct.builder().field("type", SqlTypes.STRING).build();
    metaStore.registerType("cookie", cookie);

    // When:
    final PreparedStatement<CreateSource> createSource = KsqlParserTestUtil.buildSingleAst(
        "CREATE STREAM foo (cookie COOKIE) WITH (KAFKA_TOPIC='foo', VALUE_FORMAT='AVRO');",
        metaStore
    );

    // Then:
    final TableElements elements = createSource.getStatement().getElements();
    assertThat(Iterables.size(elements), is(1));

    final TableElement element = elements.iterator().next();
    assertThat(element.getType().getSqlType(), is(cookie));
  }

  @Test
  public void shouldParseFloatingPointNumbers() {
    assertThat(parseDouble("1.23E-1"), is(new DoubleLiteral(0.123)));
    assertThat(parseDouble("1.230E+1"), is(new DoubleLiteral(12.3)));
    assertThat(parseDouble("01.23e1"), is(new DoubleLiteral(12.3)));
  }

  @Test
  public void shouldParseDecimals() {
    assertThat(parseDouble("0.1"), is(new DecimalLiteral(new BigDecimal("0.1"))));
    assertThat(parseDouble("0.123"), is(new DecimalLiteral(new BigDecimal("0.123"))));
    assertThat(parseDouble("00123.000"), is(new DecimalLiteral(new BigDecimal("123.000"))));
  }

  @Test
  public void shouldMaskParsedStatement() {
    // Given
    final String query = "--this is a comment. \n"
        + "CREATE SOURCE CONNECTOR `test-connector` WITH ("
        + "    \"connector.class\" = 'PostgresSource', \n"
        + "    'connection.url' = 'jdbc:postgresql://localhost:5432/my.db',\n"
        + "    \"mode\"='bulk',\n"
        + "    \"topic.prefix\"='jdbc-',\n"
        + "    \"table.whitelist\"='users',\n"
        + "    \"key\"='username');";

    final String masked = "CREATE SOURCE CONNECTOR `test-connector` WITH "
        + "(\"connector.class\"='PostgresSource', "
        + "'connection.url'='[string]', "
        + "\"mode\"='[string]', "
        + "\"topic.prefix\"='[string]', "
        + "\"table.whitelist\"='[string]', "
        + "\"key\"='[string]');";

    // when
    final ParsedStatement parsedStatement = ParsedStatement.of(query, mock(SingleStatementContext.class));

    // Then
    assertThat(parsedStatement.getMaskedStatementText(), is(masked));
    assertThat(parsedStatement.getUnMaskedStatementText(), is(query));
    assertThat(parsedStatement.toString(), is(masked));
  }

  @Test
  public void shouldMaskPreparedStatement() {
    // Given
    final String query = "--this is a comment. \n"
        + "CREATE SOURCE CONNECTOR `test-connector` WITH ("
        + "    \"connector.class\" = 'PostgresSource', \n"
        + "    'connection.url' = 'jdbc:postgresql://localhost:5432/my.db',\n"
        + "    \"mode\"='bulk',\n"
        + "    \"topic.prefix\"='jdbc-',\n"
        + "    \"table.whitelist\"='users',\n"
        + "    \"key\"='username');";

    final String masked = "CREATE SOURCE CONNECTOR `test-connector` WITH "
        + "(\"connector.class\"='PostgresSource', "
        + "'connection.url'='[string]', "
        + "\"mode\"='[string]', "
        + "\"topic.prefix\"='[string]', "
        + "\"table.whitelist\"='[string]', "
        + "\"key\"='[string]');";

    // when
    final PreparedStatement<CreateConnector> preparedStatement =
        PreparedStatement.of(query, mock(CreateConnector.class));

    // Then
    assertThat(preparedStatement.getMaskedStatementText(), is(masked));
    assertThat(preparedStatement.getUnMaskedStatementText(), is(query));
    assertThat(preparedStatement.toString(), is(masked));
  }

  @Test
  public void shouldThrowSyntaxErrorOnInvalidCreateStatement() {
    // Given
    final String invalidCreateStatement =
        "CRETE STREAM s1(id INT) WITH (kafka_topic='s1', value_format='JSON')";

    // When
    final ParseFailedException e = assertThrows(
        ParseFailedException.class,
        () -> KsqlParserTestUtil.parse(invalidCreateStatement)
    );

    // Then
    assertThat(e.getUnloggedMessage(), containsString(
        "line 1:1: Syntax Error\n" +
            "Unknown statement 'CRETE'\n" +
            "Did you mean 'CREATE'?"
    ));
    assertThat(e.getMessage(), containsString("line 1:1: Syntax error at line 1:1"));
    assertThat(e.getSqlStatement(), containsString("CRETE STREAM s1(id INT) WITH (kafka_topic='s1', value_format='JSON')"));
  }

  @Test
  public void shouldThrowSyntaxErrorOnMisMatchStatement() {
    // Given
    final String invalidStatement =
        "SELECT FROM FROM TEST_TOPIC";

    // When
    final ParseFailedException e = assertThrows(
        ParseFailedException.class,
        () -> KsqlParserTestUtil.parse(invalidStatement)
    );

    // Then
    assertThat(e.getUnloggedMessage(), containsString("line 1:8: Syntax Error"));
    assertThat(e.getMessage(), containsString("line 1:8: Syntax error at line 1:8"));
    assertThat(e.getSqlStatement(), containsString("SELECT FROM FROM TEST_TOPIC"));
  }

  @Test
  public void shouldThrowSyntaxErrorAndShowExpecting() {
    // Given
    final String invalidStatement =
        "SELECT * FORM TEST_TOPIC";

    // When
    final ParseFailedException e = assertThrows(
        ParseFailedException.class,
        () -> KsqlParserTestUtil.parse(invalidStatement)
    );

    // Then
    assertThat(e.getUnloggedMessage(), containsString("line 1:10: Syntax Error"));
    assertThat(e.getMessage(), containsString("line 1:10: Syntax error at line 1:10"));
    assertThat(e.getSqlStatement(), containsString("SELECT * FORM TEST_TOPIC"));
  }

  @Test
  public void shouldThrowSyntaxErrorOnWrongEOF() {
    // Given
    final String invalidStatement =
    "SELECT * FROM artist WHERE first_name = 'Vincent' and (last_name = 'Monet' or last_name = 'Da Vinci'";

    // When
    final ParseFailedException e = assertThrows(
        ParseFailedException.class,
        () -> KsqlParserTestUtil.parse(invalidStatement)
    );

    // Then
    assertThat(e.getUnloggedMessage(), containsString(
        "line 1:101: Syntax Error\n"
            + "Syntax error at or near \";\"\n"
            + "'}', ']', or ')' is missing"
    ));
    assertThat(e.getMessage(), containsString("line 1:101: Syntax error at line 1:101"));
    assertThat(e.getSqlStatement(), containsString(
        "SELECT * FROM artist WHERE first_name = 'Vincent'"
            + " and (last_name = 'Monet' or last_name = 'Da Vinci'"
    ));
  }

  @Test
  public void shouldThrowSyntaxErrorOnNoViableAltException() {
    // Given
    final String invalidCreateStatement =
        "CREATE TABLE AS SELECT x, count(*) FROM TEST_TOPIC GROUP BY x";

    // When
    final ParseFailedException e = assertThrows(
        ParseFailedException.class,
        () -> KsqlParserTestUtil.parse(invalidCreateStatement)
    );

    // Then
    assertThat(e.getUnloggedMessage(), containsString("line 1:14: Syntax Error\n" +
        "Syntax error at or near 'AS' at line 1:14"));
    assertThat(e.getMessage(), containsString("line 1:14: Syntax error at line 1:14"));
    assertThat(e.getSqlStatement(), containsString(
        "CREATE TABLE AS SELECT x, count(*) FROM TEST_TOPIC GROUP BY x"
    ));
  }

  @Test
  public void shouldGuessWhatUserMeanCorrectly() {
    // Given
    final String invalidCreateStatement =
        "sho streams;";

    // When
    final ParseFailedException e = assertThrows(
        ParseFailedException.class,
        () -> KsqlParserTestUtil.parse(invalidCreateStatement)
    );

    // Then
    assertThat(e.getUnloggedMessage(), containsString(
        "line 1:1: Syntax Error\n" +
            "Unknown statement 'sho'\n" +
            "Did you mean 'SHOW'?"
    ));
    assertThat(e.getMessage(), containsString("line 1:1: Syntax error at line 1:1"));
    assertThat(e.getSqlStatement(), containsString("sho streams;"));
  }

  private Literal parseDouble(final String literalText) {
    final PreparedStatement<Query> query = KsqlParserTestUtil
        .buildSingleAst(
            "SELECT * FROM TEST1 WHERE COL3 > " + literalText + ";",
            metaStore
        );

    final ComparisonExpression where = (ComparisonExpression) query.getStatement().getWhere().get();
    return (Literal) where.getRight();
  }

  private static SearchedCaseExpression getSearchedCaseExpressionFromCsas(final Statement statement) {
    final Query query = ((CreateStreamAsSelect) statement).getQuery();
    final Expression caseExpression = ((SingleColumn) query.getSelect().getSelectItems().get(0)).getExpression();
    return (SearchedCaseExpression) caseExpression;
  }

  private static Matcher<SelectItem> equalToColumn(final String expression) {
    return equalToColumn(expression, Optional.empty());
  }

  private static Matcher<SelectItem> equalToColumn(
      final String expression,
      final Optional<ColumnName> alias) {
    return new TypeSafeMatcher<SelectItem>() {
      @Override
      protected boolean matchesSafely(final SelectItem item) {
        if (!(item instanceof SingleColumn)) {
          return false;
        }

        final SingleColumn column = (SingleColumn) item;
        return Objects.equals(column.getExpression().toString(), expression)
            && Objects.equals(column.getAlias(), alias);
      }

      @Override
      public void describeTo(final Description description) {
        description.appendText(
            String.format("Expression: %s, Alias: %s",
                expression,
                alias));
      }
    };
  }

}
