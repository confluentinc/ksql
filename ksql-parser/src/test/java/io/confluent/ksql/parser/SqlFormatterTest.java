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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.JoinCriteria;
import io.confluent.ksql.parser.tree.JoinOn;
import io.confluent.ksql.parser.tree.ListStreams;
import io.confluent.ksql.parser.tree.ListTables;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.properties.with.CreateConfigs;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SqlFormatterTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private AliasedRelation leftAlias;
  private AliasedRelation rightAlias;
  private JoinCriteria criteria;

  private MutableMetaStore metaStore;

  private static final SourceName TEST = SourceName.of("TEST");
  private static final SourceName SOMETHING = SourceName.of("SOMETHING");

  private static final SqlType addressSchema = SqlTypes.struct()
      .field("NUMBER", SqlTypes.BIGINT)
      .field("STREET", SqlTypes.STRING)
      .field("CITY", SqlTypes.STRING)
      .field("STATE", SqlTypes.STRING)
      .field("ZIPCODE", SqlTypes.BIGINT)
      .build();

  private static final SqlType categorySchema = SqlTypes.struct()
      .field("ID", SqlTypes.BIGINT)
      .field("NAME", SqlTypes.STRING)
      .build();

  private static final SqlStruct ITEM_INFO_STRUCT = SqlStruct.builder()
      .field("ITEMID", SqlTypes.BIGINT)
      .field("NAME", SqlTypes.STRING)
      .field("CATEGORY", categorySchema)
      .build();

  private static final LogicalSchema ITEM_INFO_SCHEMA = LogicalSchema.builder()
      .keyColumn(SchemaUtil.ROWKEY_NAME, SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("ITEMID"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("NAME"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("CATEGORY"), categorySchema)
      .build();

  private static final LogicalSchema TABLE_SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("TABLE"), SqlTypes.STRING)
      .build();

  private static final LogicalSchema ORDERS_SCHEMA = LogicalSchema.builder()
      .keyColumn(SchemaUtil.ROWKEY_NAME, SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("ORDERTIME"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("ORDERID"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("ITEMID"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("ITEMINFO"), ITEM_INFO_STRUCT)
      .valueColumn(ColumnName.of("ORDERUNITS"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("ARRAYCOL"), SqlTypes.array(SqlTypes.DOUBLE))
      .valueColumn(ColumnName.of("MAPCOL"), SqlTypes.map(SqlTypes.DOUBLE))
      .valueColumn(ColumnName.of("ADDRESS"), addressSchema)
      .valueColumn(ColumnName.of("SIZE"), SqlTypes.INTEGER) // Reserved word
      .build();

  private static final CreateSourceProperties SOME_WITH_PROPS = CreateSourceProperties.from(
      ImmutableMap.of(
          CreateConfigs.KEY_NAME_PROPERTY, new StringLiteral("ORDERID"),
          CommonCreateConfigs.VALUE_FORMAT_PROPERTY, new StringLiteral("JSON"),
          CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("topic_test"))
  );

  private static final TableElements ELEMENTS_WITH_KEY = TableElements.of(
      new TableElement(Namespace.KEY, ColumnName.of("ROWKEY"), new Type(SqlTypes.STRING)),
      new TableElement(Namespace.VALUE, ColumnName.of("Foo"), new Type(SqlTypes.STRING))
  );

  private static final TableElements ELEMENTS_WITHOUT_KEY = TableElements.of(
      new TableElement(Namespace.VALUE, ColumnName.of("Foo"), new Type(SqlTypes.STRING)),
      new TableElement(Namespace.VALUE, ColumnName.of("Bar"), new Type(SqlTypes.STRING))
  );

  @Before
  public void setUp() {
    final Table left = new Table(SourceName.of("left"));
    final Table right = new Table(SourceName.of("right"));
    leftAlias = new AliasedRelation(left, SourceName.of("L"));
    rightAlias = new AliasedRelation(right, SourceName.of("R"));

    criteria = new JoinOn(new ComparisonExpression(ComparisonExpression.Type.EQUAL,
                                                   new StringLiteral("left.col0"),
                                                   new StringLiteral("right.col0")));

    metaStore = MetaStoreFixture.getNewMetaStore(mock(FunctionRegistry.class));

    final KsqlTopic ksqlTopicOrders = new KsqlTopic(
        "orders_topic",
        KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
        ValueFormat.of(FormatInfo.of(Format.JSON))
    );

    final KsqlStream ksqlStreamOrders = new KsqlStream<>(
        "sqlexpression",
        SourceName.of("ADDRESS"),
        ORDERS_SCHEMA,
        SerdeOption.none(),
        KeyField.of(ColumnRef.of(ColumnName.of("ORDERTIME"))),
        Optional.empty(),
        false,
        ksqlTopicOrders
    );

    metaStore.putSource(ksqlStreamOrders);

    final KsqlTopic ksqlTopicItems = new KsqlTopic(
        "item_topic",
        KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
        ValueFormat.of(FormatInfo.of(Format.JSON))
    );
    final KsqlTable<String> ksqlTableOrders = new KsqlTable<>(
        "sqlexpression",
        SourceName.of("ITEMID"),
        ITEM_INFO_SCHEMA,
        SerdeOption.none(),
        KeyField.of(ColumnRef.of(ColumnName.of("ITEMID"))),
        Optional.empty(),
        false,
        ksqlTopicItems
    );

    metaStore.putSource(ksqlTableOrders);

    final KsqlTable<String> ksqlTableTable = new KsqlTable<>(
        "sqlexpression",
        SourceName.of("TABLE"),
        TABLE_SCHEMA,
        SerdeOption.none(),
        KeyField.of(ColumnRef.of(ColumnName.of("TABLE"))),
        Optional.empty(),
        false,
        ksqlTopicItems
    );

    metaStore.putSource(ksqlTableTable);
  }

  @Test
  public void shouldFormatCreateStreamStatementWithExplicitKey() {
    // Given:
    final CreateStream createStream = new CreateStream(
        TEST,
        ELEMENTS_WITH_KEY,
        false,
        SOME_WITH_PROPS);

    // When:
    final String sql = SqlFormatter.formatSql(createStream);

    // Then:
    assertThat(sql, is("CREATE STREAM TEST (ROWKEY STRING KEY, `Foo` STRING) "
        + "WITH (KAFKA_TOPIC='topic_test', KEY='ORDERID', VALUE_FORMAT='JSON');"));
  }

  @Test
  public void shouldFormatCreateStreamStatementWithImplicitKey() {
    // Given:
    final CreateStream createStream = new CreateStream(
        TEST,
        ELEMENTS_WITHOUT_KEY,
        false,
        SOME_WITH_PROPS);

    // When:
    final String sql = SqlFormatter.formatSql(createStream);

    // Then:
    assertThat(sql, is("CREATE STREAM TEST (`Foo` STRING, `Bar` STRING) "
        + "WITH (KAFKA_TOPIC='topic_test', KEY='ORDERID', VALUE_FORMAT='JSON');"));
  }

  @Test
  public void shouldFormatCreateTableStatementWithExplicitTimestamp() {
    // Given:
    final CreateSourceProperties props = CreateSourceProperties.from(
        new ImmutableMap.Builder<String, Literal>()
            .putAll(SOME_WITH_PROPS.copyOfOriginalLiterals())
            .put(CommonCreateConfigs.TIMESTAMP_NAME_PROPERTY, new StringLiteral("Foo"))
            .put(CommonCreateConfigs.TIMESTAMP_FORMAT_PROPERTY, new StringLiteral("%s"))
            .build()
    );
    final CreateTable createTable = new CreateTable(
        TEST,
        ELEMENTS_WITH_KEY,
        false,
        props);

    // When:
    final String sql = SqlFormatter.formatSql(createTable);

    // Then:
    assertThat(sql, is("CREATE TABLE TEST (ROWKEY STRING KEY, `Foo` STRING) "
        + "WITH (KAFKA_TOPIC='topic_test', KEY='ORDERID', "
        + "TIMESTAMP='Foo', TIMESTAMP_FORMAT='%s', VALUE_FORMAT='JSON');"));
  }

  @Test
  public void shouldFormatCreateTableStatementWithExplicitKey() {
    // Given:
    final CreateTable createTable = new CreateTable(
        TEST,
        ELEMENTS_WITH_KEY,
        false,
        SOME_WITH_PROPS);

    // When:
    final String sql = SqlFormatter.formatSql(createTable);

    // Then:
    assertThat(sql, is("CREATE TABLE TEST (ROWKEY STRING KEY, `Foo` STRING) "
        + "WITH (KAFKA_TOPIC='topic_test', KEY='ORDERID', VALUE_FORMAT='JSON');"));
  }

  @Test
  public void shouldFormatCreateTableStatementWithImplicitKey() {
    // Given:
    final CreateTable createTable = new CreateTable(
        TEST,
        ELEMENTS_WITHOUT_KEY,
        false,
        SOME_WITH_PROPS);

    // When:
    final String sql = SqlFormatter.formatSql(createTable);

    // Then:
    assertThat(sql, is("CREATE TABLE TEST (`Foo` STRING, `Bar` STRING) "
        + "WITH (KAFKA_TOPIC='topic_test', KEY='ORDERID', VALUE_FORMAT='JSON');"));
  }

  @Test
  public void shouldFormatTableElementsNamedAfterReservedWords() {
    // Given:
    final TableElements tableElements = TableElements.of(
        new TableElement(Namespace.VALUE, ColumnName.of("GROUP"), new Type(SqlTypes.STRING)),
        new TableElement(Namespace.VALUE, ColumnName.of("Having"), new Type(SqlTypes.STRING))
    );

    final CreateStream createStream = new CreateStream(
        TEST,
        tableElements,
        false,
        SOME_WITH_PROPS);

    // When:
    final String sql = SqlFormatter.formatSql(createStream);

    // Then:
    assertThat("literal escaping failure", sql, containsString("`GROUP` STRING"));
    assertThat("lowercase literal escaping failure", sql, containsString("`Having` STRING"));

    assertValidSql(sql);
  }

  @Test
  public void shouldFormatLeftJoinWithWithin() {
    final Join join = new Join(Join.Type.LEFT, leftAlias, rightAlias,
                         criteria,
                         Optional.of(new WithinExpression(10, TimeUnit.SECONDS)));

    final String expected = "`left` L\nLEFT OUTER JOIN `right` R WITHIN 10 SECONDS ON "
                            + "(('left.col0' = 'right.col0'))";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }

  @Test
  public void shouldFormatLeftJoinWithoutJoinWindow() {
    final Join join = new Join(Join.Type.LEFT, leftAlias, rightAlias,
                               criteria, Optional.empty());

    final String result = SqlFormatter.formatSql(join);
    final String expected = "`left` L\nLEFT OUTER JOIN `right` R ON (('left.col0' = 'right.col0'))";
    assertEquals(expected, result);
  }

  @Test
  public void shouldFormatInnerJoin() {
    final Join join = new Join(Join.Type.INNER, leftAlias, rightAlias,
                               criteria,
                               Optional.of(new WithinExpression(10, TimeUnit.SECONDS)));

    final String expected = "`left` L\nINNER JOIN `right` R WITHIN 10 SECONDS ON "
                            + "(('left.col0' = 'right.col0'))";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }

  @Test
  public void shouldFormatInnerJoinWithoutJoinWindow() {
    final Join join = new Join(Join.Type.INNER, leftAlias, rightAlias,
                               criteria,
                               Optional.empty());

    final String expected = "`left` L\nINNER JOIN `right` R ON (('left.col0' = 'right.col0'))";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }


  @Test
  public void shouldFormatOuterJoin() {
    final Join join = new Join(Join.Type.OUTER, leftAlias, rightAlias,
                               criteria,
                               Optional.of(new WithinExpression(10, TimeUnit.SECONDS)));

    final String expected = "`left` L\nFULL OUTER JOIN `right` R WITHIN 10 SECONDS ON"
                            + " (('left.col0' = 'right.col0'))";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }


  @Test
  public void shouldFormatOuterJoinWithoutJoinWindow() {
    final Join join = new Join(Join.Type.OUTER, leftAlias, rightAlias,
                               criteria,
                               Optional.empty());

    final String expected = "`left` L\nFULL OUTER JOIN `right` R ON (('left.col0' = 'right.col0'))";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }

  @Test
  public void shouldFormatSelectQueryCorrectly() {
    final String statementString =
        "CREATE STREAM S AS SELECT a.address->city FROM address a;";
    final Statement statement = parseSingle(statementString);
    assertThat(SqlFormatter.formatSql(statement), equalTo("CREATE STREAM S AS SELECT A.ADDRESS->CITY\n"
        + "FROM ADDRESS A\nEMIT CHANGES"));
  }

  @Test
  public void shouldFormatSelectStarCorrectly() {
    final String statementString = "CREATE STREAM S AS SELECT * FROM address;";
    final Statement statement = parseSingle(statementString);
    assertThat(SqlFormatter.formatSql(statement),
        equalTo("CREATE STREAM S AS SELECT *\n"
            + "FROM ADDRESS ADDRESS\nEMIT CHANGES"));
  }

  @Test
  public void shouldFormatCSASWithReservedWords() {
    final String statementString = "CREATE STREAM S AS SELECT ITEMID, \"SIZE\" FROM address;";
    final Statement statement = parseSingle(statementString);
    assertThat(SqlFormatter.formatSql(statement),
        equalTo("CREATE STREAM S AS SELECT\n" +
            "  ITEMID,\n  `SIZE`\nFROM ADDRESS ADDRESS\nEMIT CHANGES"));
  }

  @Test
  public void shouldFormatSelectWithReservedWordAlias() {
    final String statementString = "CREATE STREAM S AS SELECT address AS `STREAM` FROM address;";
    final Statement statement = parseSingle(statementString);
    assertThat(SqlFormatter.formatSql(statement),
        equalTo("CREATE STREAM S AS SELECT"
            + " ADDRESS `STREAM`\n"
            + "FROM ADDRESS ADDRESS\nEMIT CHANGES"));
  }

  @Test
  public void shouldFormatSelectWithLowerCaseAlias() {
    final String statementString = "CREATE STREAM S AS SELECT address AS `foO` FROM address;";
    final Statement statement = parseSingle(statementString);
    assertThat(SqlFormatter.formatSql(statement),
        equalTo("CREATE STREAM S AS SELECT"
            + " ADDRESS `foO`\n"
            + "FROM ADDRESS ADDRESS\nEMIT CHANGES"));
  }

  @Test
  public void shouldFormatSelectStarCorrectlyWithOtherFields() {
    final String statementString = "CREATE STREAM S AS SELECT *, address AS city FROM address;";
    final Statement statement = parseSingle(statementString);
    assertThat(SqlFormatter.formatSql(statement),
        equalTo("CREATE STREAM S AS SELECT\n"
            + "  *,\n"
            + "  ADDRESS CITY\n"
            + "FROM ADDRESS ADDRESS\nEMIT CHANGES"));
  }

  @Test
  public void shouldFormatSelectStarCorrectlyWithJoin() {
    final String statementString = "CREATE STREAM S AS SELECT address.*, itemid.* "
        + "FROM address INNER JOIN itemid ON address.address = itemid.address->address;";
    final Statement statement = parseSingle(statementString);
    assertThat(SqlFormatter.formatSql(statement),
        equalTo("CREATE STREAM S AS SELECT\n"
            + "  ADDRESS.*,\n"
            + "  ITEMID.*\n"
            + "FROM ADDRESS ADDRESS\n"
            + "INNER JOIN ITEMID ITEMID ON ((ADDRESS.ADDRESS = ITEMID.ADDRESS->ADDRESS))\n"
            + "EMIT CHANGES"));
  }

  @Test
  public void shouldFormatSelectStarCorrectlyWithJoinOneSidedStar() {
    final String statementString = "CREATE STREAM S AS SELECT address.*, itemid.ordertime "
        + "FROM address INNER JOIN itemid ON address.address = itemid.address->address;";
    final Statement statement = parseSingle(statementString);
    assertThat(SqlFormatter.formatSql(statement),
        equalTo("CREATE STREAM S AS SELECT\n"
            + "  ADDRESS.*,\n"
            + "  ITEMID.ORDERTIME\n"
            + "FROM ADDRESS ADDRESS\n"
            + "INNER JOIN ITEMID ITEMID ON ((ADDRESS.ADDRESS = ITEMID.ADDRESS->ADDRESS))\n"
            + "EMIT CHANGES"));
  }

  @Test
  public void shouldFormatSelectCorrectlyWithDuplicateFields() {
    final String statementString = "CREATE STREAM S AS SELECT address AS one, address AS two FROM address;";
    final Statement statement = parseSingle(statementString);
    assertThat(SqlFormatter.formatSql(statement),
        equalTo("CREATE STREAM S AS SELECT\n"
            + "  ADDRESS ONE,\n"
            + "  ADDRESS TWO\n"
            + "FROM ADDRESS ADDRESS\n"
            + "EMIT CHANGES"));
  }

  @Test
  public void shouldFormatCsasWithClause() {
    final String statementString = "CREATE STREAM S WITH(partitions=4) AS SELECT * FROM address;";
    final Statement statement = parseSingle(statementString);

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, startsWith("CREATE STREAM S WITH (PARTITIONS=4) AS SELECT"));
  }

  @Test
  public void shouldFormatCtasWithClause() {
    final String statementString = "CREATE TABLE S WITH(partitions=4) AS SELECT * FROM address;";
    final Statement statement = parseSingle(statementString);

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, startsWith("CREATE TABLE S WITH (PARTITIONS=4) AS SELECT"));
  }

  @Test
  public void shouldFormatCsasPartitionBy() {
    final String statementString = "CREATE STREAM S AS SELECT * FROM ADDRESS PARTITION BY ADDRESS;";
    final Statement statement = parseSingle(statementString);

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, is("CREATE STREAM S AS SELECT *\n"
        + "FROM ADDRESS ADDRESS\n"
        + "PARTITION BY ADDRESS\n"
        + "EMIT CHANGES"
    ));
  }

  @Test
  public void shouldFormatInsertIntoPartitionBy() {
    final String statementString = "INSERT INTO ADDRESS SELECT * FROM ADDRESS PARTITION BY ADDRESS;";
    final Statement statement = parseSingle(statementString);

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, startsWith("INSERT INTO ADDRESS SELECT *\n"
        + "FROM ADDRESS ADDRESS\n"
        + "PARTITION BY ADDRESS\n"
        + "EMIT CHANGES"
    ));
  }

  @Test
  public void shouldFormatExplainQuery() {
    final String statementString = "EXPLAIN foo;";
    final Statement statement = parseSingle(statementString);

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, is("EXPLAIN \nfoo"));
  }

  @Test
  public void shouldFormatExplainStatement() {
    final String statementString = "EXPLAIN SELECT * FROM ADDRESS;";
    final Statement statement = parseSingle(statementString);

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, is("EXPLAIN \nSELECT *\nFROM ADDRESS ADDRESS"));
  }

  @Test
  public void shouldFormatDropStreamStatementIfExistsDeleteTopic() {
    // Given:
    final DropStream dropStream = new DropStream(SOMETHING, true, true);

    // When:
    final String formatted = SqlFormatter.formatSql(dropStream);

    // Then:
    assertThat(formatted, is("DROP STREAM IF EXISTS SOMETHING DELETE TOPIC"));
  }

  @Test
  public void shouldFormatDropStreamStatementIfExists() {
    // Given:
    final DropStream dropStream = new DropStream(SOMETHING, true, false);

    // When:
    final String formatted = SqlFormatter.formatSql(dropStream);

    // Then:
    assertThat(formatted, is("DROP STREAM IF EXISTS SOMETHING"));
  }

  @Test
  public void shouldFormatDropStreamStatement() {
    // Given:
    final DropStream dropStream = new DropStream(SOMETHING, false, false);

    // When:
    final String formatted = SqlFormatter.formatSql(dropStream);

    // Then:
    assertThat(formatted, is("DROP STREAM SOMETHING"));
  }

  @Test
  public void shouldFormatDropTableStatement() {
    // Given:
    final DropTable dropStream = new DropTable(SOMETHING, false, false);

    // When:
    final String formatted = SqlFormatter.formatSql(dropStream);

    // Then:
    assertThat(formatted, is("DROP TABLE SOMETHING"));
  }

  @Test
  public void shouldFormatTerminateQuery() {
    // Given:
    final TerminateQuery terminateQuery = TerminateQuery.query(Optional.empty(), new QueryId("FOO"));

    // When:
    final String formatted = SqlFormatter.formatSql(terminateQuery);

    // Then:
    assertThat(formatted, is("TERMINATE FOO"));
  }

  @Test
  public void shouldFormatTerminateAllQueries() {
    // Given:
    final TerminateQuery terminateQuery = TerminateQuery.all(Optional.empty());

    // When:
    final String formatted = SqlFormatter.formatSql(terminateQuery);

    // Then:
    assertThat(formatted, is("TERMINATE ALL"));
  }

  @Test
  public void shouldFormatShowTables() {
    // Given:
    final ListTables listTables = new ListTables(Optional.empty(), false);

    // When:
    final String formatted = SqlFormatter.formatSql(listTables);

    // Then:
    assertThat(formatted, is("SHOW TABLES"));
  }

  @Test
  public void shouldFormatShowTablesExtended() {
    // Given:
    final ListTables listTables = new ListTables(Optional.empty(), true);

    // When:
    final String formatted = SqlFormatter.formatSql(listTables);

    // Then:
    assertThat(formatted, is("SHOW TABLES EXTENDED"));
  }

  @Test
  public void shouldFormatShowStreams() {
    // Given:
    final ListStreams listStreams = new ListStreams(Optional.empty(), false);

    // When:
    final String formatted = SqlFormatter.formatSql(listStreams);

    // Then:
    assertThat(formatted, is("SHOW STREAMS"));
  }

  @Test
  public void shouldFormatShowStreamsExtended() {
    // Given:
    final ListStreams listStreams = new ListStreams(Optional.empty(), true);

    // When:
    final String formatted = SqlFormatter.formatSql(listStreams);

    // Then:
    assertThat(formatted, is("SHOW STREAMS EXTENDED"));
  }

  @Test
  public void shouldFormatInsertValuesStatement() {
    final String statementString = "INSERT INTO ADDRESS (NUMBER, STREET, CITY) VALUES (2, 'high', 'palo alto');";
    final Statement statement = parseSingle(statementString);

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, is("INSERT INTO ADDRESS (NUMBER, STREET, CITY) VALUES (2, 'high', 'palo alto')"));
  }

  @Test
  public void shouldFormatInsertValuesNoSchema() {
    final String statementString = "INSERT INTO ADDRESS VALUES (2);";
    final Statement statement = parseSingle(statementString);

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, is("INSERT INTO ADDRESS VALUES (2)"));
  }

  @Test
  public void shouldParseArbitraryExpressions() {
    // Given:
    final String statementString = "INSERT INTO ADDRESS VALUES (2 + 1);";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore).getStatement();

    // When:
    final String result = SqlFormatter.formatSql(statement);

    // Then:
    assertThat(result, is("INSERT INTO ADDRESS VALUES ((2 + 1))"));
  }

  @Test
  public void shouldFormatTumblingWindow() {
    // Given:
    final String statementString = "CREATE STREAM S AS SELECT ITEMID, COUNT(*) FROM ORDERS WINDOW TUMBLING (SIZE 7 DAYS) GROUP BY ITEMID;";
    final Statement statement = parseSingle(statementString);

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, is("CREATE STREAM S AS SELECT\n"
        + "  ITEMID,\n"
        + "  COUNT(*)\n"
        + "FROM ORDERS ORDERS\n"
        + "WINDOW TUMBLING ( SIZE 7 DAYS ) \n"
        + "GROUP BY ITEMID\n"
        + "EMIT CHANGES"));
  }

  @Test
  public void shouldFormatHoppingWindow() {
    // Given:
    final String statementString = "CREATE STREAM S AS SELECT ITEMID, COUNT(*) FROM ORDERS WINDOW HOPPING (SIZE 20 SECONDS, ADVANCE BY 5 SECONDS) GROUP BY ITEMID;";
    final Statement statement = parseSingle(statementString);

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, is("CREATE STREAM S AS SELECT\n"
        + "  ITEMID,\n"
        + "  COUNT(*)\n"
        + "FROM ORDERS ORDERS\n"
        + "WINDOW HOPPING ( SIZE 20 SECONDS , ADVANCE BY 5 SECONDS ) \n"
        + "GROUP BY ITEMID\n"
        + "EMIT CHANGES"));
  }

  @Test
  public void shouldFormatSessionWindow() {
    // Given:
    final Statement statement = parseSingle(
        "CREATE STREAM S AS SELECT ITEMID, COUNT(*) FROM ORDERS WINDOW SESSION (15 MINUTES) GROUP BY ITEMID;");

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, is("CREATE STREAM S AS SELECT\n"
        + "  ITEMID,\n"
        + "  COUNT(*)\n"
        + "FROM ORDERS ORDERS\n"
        + "WINDOW SESSION ( 15 MINUTES ) \n"
        + "GROUP BY ITEMID\n"
        + "EMIT CHANGES"));
  }

  @Test
  public void shouldFormatDescribeSource() {
    // Given:
    final Statement statement = parseSingle("DESCRIBE ORDERS;");

    // When:
    final String result = SqlFormatter.formatSql(statement);

    // Then:
    assertThat(result, is("DESCRIBE ORDERS"));
  }

  @Test
  public void shouldFormatStructWithReservedWords() {
    // Given:
    final Statement statement = parseSingle("CREATE STREAM s (foo STRUCT<`END` VARCHAR>) WITH (kafka_topic='foo', value_format='JSON');");

    // When:
    final String result = SqlFormatter.formatSql(statement);

    // Then:
    assertThat(result, is("CREATE STREAM S (FOO STRUCT<`END` STRING>) WITH (KAFKA_TOPIC='foo', VALUE_FORMAT='JSON');"));
  }

  @Test
  public void shouldEscapeReservedSourceNames() {
    // Given:
    final Statement statement = parseSingle("CREATE STREAM `SELECT` (foo VARCHAR) WITH (kafka_topic='foo', value_format='JSON');");

    // When:
    final String result = SqlFormatter.formatSql(statement);

    // Then:
    assertThat(result, is("CREATE STREAM `SELECT` (FOO STRING) WITH (KAFKA_TOPIC='foo', VALUE_FORMAT='JSON');"));
  }

  @Test
  public void shouldEscapeReservedNameAndAlias() {
    // Given:
    final Statement statement = parseSingle("CREATE STREAM a AS SELECT `SELECT` FROM `TABLE`;");

    // When:
    final String result = SqlFormatter.formatSql(statement);

    // Then:
    assertThat(result,
        is("CREATE STREAM A AS SELECT `SELECT`\nFROM `TABLE` `TABLE`\nEMIT CHANGES"));
  }

  @Test
  public void shouldSupportExplicitEmitChangesOnBareQuery() {
    // Given:
    final Statement statement = parseSingle("SELECT ITEMID FROM ORDERS EMIT CHANGES;");

    // When:
    final String result = SqlFormatter.formatSql(statement);

    // Then:
    assertThat(result, is("SELECT ITEMID\n"
        + "FROM ORDERS ORDERS\n"
        + "EMIT CHANGES"));
  }

  @Test
  public void shouldSupportExplicitEmitChangesOnPersistentQuery() {
    // Given:
    final Statement statement = parseSingle("CREATE STREAM X AS SELECT ITEMID FROM ORDERS EMIT CHANGES;");

    // When:
    final String result = SqlFormatter.formatSql(statement);

    // Then:
    assertThat(result, is("CREATE STREAM X AS SELECT ITEMID\n"
        + "FROM ORDERS ORDERS\n"
        + "EMIT CHANGES"));
  }

  @Test
  public void shouldSupportImplicitEmitChangesOnPersistentQuery() {
    // Given:
    final Statement statement = parseSingle("CREATE STREAM X AS SELECT ITEMID FROM ORDERS;");

    // When:
    final String result = SqlFormatter.formatSql(statement);

    // Then:
    assertThat(result, is("CREATE STREAM X AS SELECT ITEMID\n"
        + "FROM ORDERS ORDERS\n"
        + "EMIT CHANGES"));
  }

  private Statement parseSingle(final String statementString) {
    return KsqlParserTestUtil.buildSingleAst(statementString, metaStore).getStatement();
  }

  private void assertValidSql(final String sql) {
    // Will throw if invalid
    KsqlParserTestUtil.buildAst(sql, metaStore);
  }
}
