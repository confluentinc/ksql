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
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.JoinCriteria;
import io.confluent.ksql.parser.tree.JoinOn;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.properties.with.CreateConfigs;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class SqlFormatterTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private AliasedRelation leftAlias;
  private AliasedRelation rightAlias;
  private JoinCriteria criteria;

  private MutableMetaStore metaStore;

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

  private static final LogicalSchema itemInfoSchema = LogicalSchema.builder()
      .valueField("ITEMID", SqlTypes.BIGINT)
      .valueField("NAME", SqlTypes.STRING)
      .valueField("CATEGORY", categorySchema)
      .build();

  private static final LogicalSchema ORDERS_SCHEMA = LogicalSchema.builder()
      .valueField("ORDERTIME", SqlTypes.BIGINT)
      .valueField("ORDERID", SqlTypes.BIGINT)
      .valueField("ITEMID", SqlTypes.STRING)
      .valueField("ITEMINFO", SqlTypes
          .struct()
          .fields(itemInfoSchema.valueFields())
          .build())
      .valueField("ORDERUNITS", SqlTypes.INTEGER)
      .valueField("ARRAYCOL", SqlTypes.array(SqlTypes.DOUBLE))
      .valueField("MAPCOL", SqlTypes.map(SqlTypes.DOUBLE))
      .valueField("ADDRESS", addressSchema)
      .build();

  private static final CreateSourceProperties SOME_WITH_PROPS = CreateSourceProperties.from(
      ImmutableMap.of(
          CreateConfigs.KEY_NAME_PROPERTY, new StringLiteral("ORDERID"),
          CommonCreateConfigs.VALUE_FORMAT_PROPERTY, new StringLiteral("JSON"),
          CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("topic_test"))
  );

  private static final TableElements ELEMENTS_WITH_KEY = TableElements.of(
      new TableElement(Namespace.KEY, "ROWKEY", new Type(SqlTypes.STRING)),
      new TableElement(Namespace.VALUE, "Foo", new Type(SqlTypes.STRING))
  );

  private static final TableElements ELEMENTS_WITHOUT_KEY = TableElements.of(
      new TableElement(Namespace.VALUE, "Foo", new Type(SqlTypes.STRING)),
      new TableElement(Namespace.VALUE, "Bar", new Type(SqlTypes.STRING))
  );

  @Before
  public void setUp() {
    final Table left = new Table(QualifiedName.of(Collections.singletonList("left")));
    final Table right = new Table(QualifiedName.of(Collections.singletonList("right")));
    leftAlias = new AliasedRelation(left, "l");
    rightAlias = new AliasedRelation(right, "r");

    criteria = new JoinOn(new ComparisonExpression(ComparisonExpression.Type.EQUAL,
                                                   new StringLiteral("left.col0"),
                                                   new StringLiteral("right.col0")));

    metaStore = MetaStoreFixture.getNewMetaStore(mock(FunctionRegistry.class));

    final KsqlTopic ksqlTopicOrders = new KsqlTopic(
        "orders_topic",
        KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
        ValueFormat.of(FormatInfo.of(Format.JSON)),
        false
    );

    final KsqlStream ksqlStreamOrders = new KsqlStream<>(
        "sqlexpression",
        "ADDRESS",
        ORDERS_SCHEMA,
        SerdeOption.none(),
        KeyField.of("ORDERTIME", ORDERS_SCHEMA.findValueField("ORDERTIME").get()),
        new MetadataTimestampExtractionPolicy(),
        ksqlTopicOrders
    );

    metaStore.putSource(ksqlStreamOrders);

    final KsqlTopic ksqlTopicItems = new KsqlTopic(
        "item_topic",
        KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
        ValueFormat.of(FormatInfo.of(Format.JSON)),
        false
    );
    final KsqlTable<String> ksqlTableOrders = new KsqlTable<>(
        "sqlexpression",
        "ITEMID",
        itemInfoSchema,
        SerdeOption.none(),
        KeyField.of("ITEMID", itemInfoSchema.findValueField("ITEMID").get()),
        new MetadataTimestampExtractionPolicy(),
        ksqlTopicItems
    );

    metaStore.putSource(ksqlTableOrders);
  }

  @Test
  public void shouldFormatCreateStreamStatementWithExplicitKey() {
    // Given:
    final CreateStream createStream = new CreateStream(
        QualifiedName.of("TEST"),
        ELEMENTS_WITH_KEY,
        false,
        SOME_WITH_PROPS);

    // When:
    final String sql = SqlFormatter.formatSql(createStream);

    // Then:
    assertThat(sql, is("CREATE STREAM TEST (ROWKEY STRING KEY, Foo STRING) "
        + "WITH (KAFKA_TOPIC='topic_test', KEY='ORDERID', VALUE_FORMAT='JSON');"));
  }

  @Test
  public void shouldFormatCreateStreamStatementWithImplicitKey() {
    // Given:
    final CreateStream createStream = new CreateStream(
        QualifiedName.of("TEST"),
        ELEMENTS_WITHOUT_KEY,
        false,
        SOME_WITH_PROPS);

    // When:
    final String sql = SqlFormatter.formatSql(createStream);

    // Then:
    assertThat(sql, is("CREATE STREAM TEST (Foo STRING, Bar STRING) "
        + "WITH (KAFKA_TOPIC='topic_test', KEY='ORDERID', VALUE_FORMAT='JSON');"));
  }

  @Test
  public void shouldFormatCreateTableStatementWithExplicitKey() {
    // Given:
    final CreateTable createTable = new CreateTable(
        QualifiedName.of("TEST"),
        ELEMENTS_WITH_KEY,
        false,
        SOME_WITH_PROPS);

    // When:
    final String sql = SqlFormatter.formatSql(createTable);

    // Then:
    assertThat(sql, is("CREATE TABLE TEST (ROWKEY STRING KEY, Foo STRING) "
        + "WITH (KAFKA_TOPIC='topic_test', KEY='ORDERID', VALUE_FORMAT='JSON');"));
  }

  @Test
  public void shouldFormatCreateTableStatementWithImplicitKey() {
    // Given:
    final CreateTable createTable = new CreateTable(
        QualifiedName.of("TEST"),
        ELEMENTS_WITHOUT_KEY,
        false,
        SOME_WITH_PROPS);

    // When:
    final String sql = SqlFormatter.formatSql(createTable);

    // Then:
    assertThat(sql, is("CREATE TABLE TEST (Foo STRING, Bar STRING) "
        + "WITH (KAFKA_TOPIC='topic_test', KEY='ORDERID', VALUE_FORMAT='JSON');"));
  }

  @Test
  public void shouldFormatTableElementsNamedAfterReservedWords() {
    // Given:
    final TableElements tableElements = TableElements.of(
        new TableElement(Namespace.VALUE, "GROUP", new Type(SqlTypes.STRING)),
        new TableElement(Namespace.VALUE, "Having", new Type(SqlTypes.STRING))
    );

    final CreateStream createStream = new CreateStream(
        QualifiedName.of("TEST"),
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

    final String expected = "left L\nLEFT OUTER JOIN right R WITHIN 10 SECONDS ON "
                            + "(('left.col0' = 'right.col0'))";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }

  @Test
  public void shouldFormatLeftJoinWithoutJoinWindow() {
    final Join join = new Join(Join.Type.LEFT, leftAlias, rightAlias,
                               criteria, Optional.empty());

    final String result = SqlFormatter.formatSql(join);
    final String expected = "left L\nLEFT OUTER JOIN right R ON (('left.col0' = 'right.col0'))";
    assertEquals(expected, result);
  }

  @Test
  public void shouldFormatInnerJoin() {
    final Join join = new Join(Join.Type.INNER, leftAlias, rightAlias,
                               criteria,
                               Optional.of(new WithinExpression(10, TimeUnit.SECONDS)));

    final String expected = "left L\nINNER JOIN right R WITHIN 10 SECONDS ON "
                            + "(('left.col0' = 'right.col0'))";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }

  @Test
  public void shouldFormatInnerJoinWithoutJoinWindow() {
    final Join join = new Join(Join.Type.INNER, leftAlias, rightAlias,
                               criteria,
                               Optional.empty());

    final String expected = "left L\nINNER JOIN right R ON (('left.col0' = 'right.col0'))";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }


  @Test
  public void shouldFormatOuterJoin() {
    final Join join = new Join(Join.Type.OUTER, leftAlias, rightAlias,
                               criteria,
                               Optional.of(new WithinExpression(10, TimeUnit.SECONDS)));

    final String expected = "left L\nFULL OUTER JOIN right R WITHIN 10 SECONDS ON"
                            + " (('left.col0' = 'right.col0'))";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }


  @Test
  public void shouldFormatOuterJoinWithoutJoinWindow() {
    final Join join = new Join(Join.Type.OUTER, leftAlias, rightAlias,
                               criteria,
                               Optional.empty());

    final String expected = "left L\nFULL OUTER JOIN right R ON (('left.col0' = 'right.col0'))";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }

  @Test
  public void shouldFormatSelectQueryCorrectly() {
    final String statementString =
        "CREATE STREAM S AS SELECT a.address->city FROM address a;";
    final Statement statement = parseSingle(statementString);
    assertThat(SqlFormatter.formatSql(statement), equalTo("CREATE STREAM S AS SELECT FETCH_FIELD_FROM_STRUCT(A.ADDRESS, 'CITY') \"ADDRESS__CITY\"\n"
        + "FROM ADDRESS A"));
  }

  @Test
  public void shouldFormatSelectStarCorrectly() {
    final String statementString = "CREATE STREAM S AS SELECT * FROM address;";
    final Statement statement = parseSingle(statementString);
    assertThat(SqlFormatter.formatSql(statement),
        equalTo("CREATE STREAM S AS SELECT *\n"
            + "FROM ADDRESS ADDRESS"));
  }

  @Test
  public void shouldFormatSelectStarCorrectlyWithOtherFields() {
    final String statementString = "CREATE STREAM S AS SELECT *, address AS city FROM address;";
    final Statement statement = parseSingle(statementString);
    assertThat(SqlFormatter.formatSql(statement),
        equalTo("CREATE STREAM S AS SELECT\n"
            + "  *,\n"
            + "  ADDRESS.ADDRESS \"CITY\"\n"
            + "FROM ADDRESS ADDRESS"));
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
            + "INNER JOIN ITEMID ITEMID ON ((ADDRESS.ADDRESS = ITEMID.ADDRESS->ADDRESS))"));
  }

  @Test
  public void shouldFormatSelectStarCorrectlyWithJoinOneSidedStar() {
    final String statementString = "CREATE STREAM S AS SELECT address.*, itemid.ordertime "
        + "FROM address INNER JOIN itemid ON address.address = itemid.address->address;";
    final Statement statement = parseSingle(statementString);
    assertThat(SqlFormatter.formatSql(statement),
        equalTo("CREATE STREAM S AS SELECT\n"
            + "  ADDRESS.*,\n"
            + "  ITEMID.ORDERTIME \"ORDERTIME\"\n"
            + "FROM ADDRESS ADDRESS\n"
            + "INNER JOIN ITEMID ITEMID ON ((ADDRESS.ADDRESS = ITEMID.ADDRESS->ADDRESS))"));
  }

  @Test
  public void shouldFormatSelectCorrectlyWithDuplicateFields() {
    final String statementString = "CREATE STREAM S AS SELECT address AS one, address AS two FROM address;";
    final Statement statement = parseSingle(statementString);
    assertThat(SqlFormatter.formatSql(statement),
        equalTo("CREATE STREAM S AS SELECT\n"
            + "  ADDRESS.ADDRESS \"ONE\",\n"
            + "  ADDRESS.ADDRESS \"TWO\"\n"
            + "FROM ADDRESS ADDRESS"));
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

    assertThat(result, startsWith("CREATE STREAM S AS SELECT *\n"
        + "FROM ADDRESS ADDRESS\n"
        + "PARTITION BY ADDRESS"));
  }

  @Test
  public void shouldFormatInsertIntoPartitionBy() {
    final String statementString = "INSERT INTO ADDRESS SELECT * FROM ADDRESS PARTITION BY ADDRESS;";
    final Statement statement = parseSingle(statementString);

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, startsWith("INSERT INTO ADDRESS SELECT *\n"
        + "FROM ADDRESS ADDRESS\n"
        + "PARTITION BY ADDRESS"));
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
    final DropStream dropStream = new DropStream(QualifiedName.of("SOMETHING"), true, true);

    // When:
    final String formatted = SqlFormatter.formatSql(dropStream);

    // Then:
    assertThat(formatted, is("DROP STREAM IF EXISTS SOMETHING DELETE TOPIC"));
  }

  @Test
  public void shouldFormatDropStreamStatementIfExists() {
    // Given:
    final DropStream dropStream = new DropStream(QualifiedName.of("SOMETHING"), true, false);

    // When:
    final String formatted = SqlFormatter.formatSql(dropStream);

    // Then:
    assertThat(formatted, is("DROP STREAM IF EXISTS SOMETHING"));
  }

  @Test
  public void shouldFormatDropStreamStatement() {
    // Given:
    final DropStream dropStream = new DropStream(QualifiedName.of("SOMETHING"), false, false);

    // When:
    final String formatted = SqlFormatter.formatSql(dropStream);

    // Then:
    assertThat(formatted, is("DROP STREAM SOMETHING"));
  }

  @Test
  public void shouldFormatDropTableStatement() {
    // Given:
    final DropTable dropStream = new DropTable(QualifiedName.of("SOMETHING"), false, false);

    // When:
    final String formatted = SqlFormatter.formatSql(dropStream);

    // Then:
    assertThat(formatted, is("DROP TABLE SOMETHING"));
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
  public void shouldNotParseArbitraryExpressions() {
    // Given:
    final String statementString = "INSERT INTO ADDRESS VALUES (2 + 1);";

    // Expect:
    expectedException.expect(ParseFailedException.class);
    expectedException.expectMessage("mismatched input");

    // When:
    KsqlParserTestUtil.buildSingleAst(statementString, metaStore);
  }

  @Test
  public void shouldFormatTumblingWindow() {
    // Given:
    final String statementString = "CREATE STREAM S AS SELECT ITEMID, COUNT(*) FROM ORDERS WINDOW TUMBLING (SIZE 7 DAYS) GROUP BY ITEMID;";
    final Statement statement = parseSingle(statementString);

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, is("CREATE STREAM S AS SELECT\n"
        + "  ORDERS.ITEMID \"ITEMID\",\n"
        + "  COUNT(*) \"KSQL_COL_1\"\n"
        + "FROM ORDERS ORDERS\n"
        + "WINDOW TUMBLING ( SIZE 7 DAYS ) \n"
        + "GROUP BY ORDERS.ITEMID"));
  }

  @Test
  public void shouldFormatHoppingWindow() {
    // Given:
    final String statementString = "CREATE STREAM S AS SELECT ITEMID, COUNT(*) FROM ORDERS WINDOW HOPPING (SIZE 20 SECONDS, ADVANCE BY 5 SECONDS) GROUP BY ITEMID;";
    final Statement statement = parseSingle(statementString);

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, is("CREATE STREAM S AS SELECT\n"
        + "  ORDERS.ITEMID \"ITEMID\",\n"
        + "  COUNT(*) \"KSQL_COL_1\"\n"
        + "FROM ORDERS ORDERS\n"
        + "WINDOW HOPPING ( SIZE 20 SECONDS , ADVANCE BY 5 SECONDS ) \n"
        + "GROUP BY ORDERS.ITEMID"));
  }

  @Test
  public void shouldFormatSessionWindow() {
    // Given:
    final Statement statement = parseSingle(
        "CREATE STREAM S AS SELECT ITEMID, COUNT(*) FROM ORDERS WINDOW SESSION (15 MINUTES) GROUP BY ITEMID;");

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, is("CREATE STREAM S AS SELECT\n"
        + "  ORDERS.ITEMID \"ITEMID\",\n"
        + "  COUNT(*) \"KSQL_COL_1\"\n"
        + "FROM ORDERS ORDERS\n"
        + "WINDOW SESSION ( 15 MINUTES ) \n"
        + "GROUP BY ORDERS.ITEMID"));
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

  private Statement parseSingle(final String statementString) {
    return KsqlParserTestUtil.buildSingleAst(statementString, metaStore).getStatement();
  }

  private void assertValidSql(final String sql) {
    // Will throw if invalid
    KsqlParserTestUtil.buildAst(sql, metaStore);
  }
}
