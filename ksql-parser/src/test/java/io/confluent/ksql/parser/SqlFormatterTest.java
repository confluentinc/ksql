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
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.exception.ParseFailedException;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.JoinCriteria;
import io.confluent.ksql.parser.tree.JoinOn;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SqlType;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.json.KsqlJsonSerdeFactory;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
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

  private static final Schema addressSchema = SchemaBuilder.struct()
      .field("NUMBER", Schema.OPTIONAL_INT64_SCHEMA)
      .field("STREET", Schema.OPTIONAL_STRING_SCHEMA)
      .field("CITY", Schema.OPTIONAL_STRING_SCHEMA)
      .field("STATE", Schema.OPTIONAL_STRING_SCHEMA)
      .field("ZIPCODE", Schema.OPTIONAL_INT64_SCHEMA)
      .optional().build();

  private static final Schema categorySchema = SchemaBuilder.struct()
      .field("ID", Schema.OPTIONAL_INT64_SCHEMA)
      .field("NAME", Schema.OPTIONAL_STRING_SCHEMA)
      .optional().build();

  private static final Schema itemInfoSchema = SchemaBuilder.struct()
      .field("ITEMID", Schema.OPTIONAL_INT64_SCHEMA)
      .field("NAME", Schema.OPTIONAL_STRING_SCHEMA)
      .field("CATEGORY", categorySchema)
      .optional().build();

  private static final Schema schemaBuilderOrders = SchemaBuilder.struct()
      .field("ROWTIME", Schema.OPTIONAL_INT64_SCHEMA)
      .field("ROWKEY", Schema.OPTIONAL_STRING_SCHEMA)
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

    final KsqlTopic
        ksqlTopicOrders =
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

    metaStore.putTopic(ksqlTopicOrders);
    metaStore.putSource(ksqlStreamOrders);

    final KsqlTopic
        ksqlTopicItems =
        new KsqlTopic("ITEMS_TOPIC", "item_topic", new KsqlJsonSerdeFactory(), false);
    final KsqlTable<String> ksqlTableOrders = new KsqlTable<>(
        "sqlexpression",
        "ITEMID",
        LogicalSchema.of(itemInfoSchema).withImplicitAndKeyFieldsInValue(),
        SerdeOption.none(),
        KeyField.of("ITEMID", itemInfoSchema.field("ITEMID")),
        new MetadataTimestampExtractionPolicy(),
        ksqlTopicItems,
        Serdes::String
    );
    metaStore.putTopic(ksqlTopicItems);
    metaStore.putSource(ksqlTableOrders);
  }

  @Test
  public void testFormatSql() {

    final ArrayList<TableElement> tableElements = new ArrayList<>();
    tableElements.add(new TableElement("GROUP", PrimitiveType.of(SqlType.STRING)));
    tableElements.add(new TableElement("NOLIT", PrimitiveType.of(SqlType.STRING)));
    tableElements.add(new TableElement("Having", PrimitiveType.of(SqlType.STRING)));

    final CreateStream createStream = new CreateStream(
        QualifiedName.of("TEST"),
        tableElements,
        false,
        ImmutableMap.of(
            DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("topic_test"),
            DdlConfig.VALUE_FORMAT_PROPERTY, new StringLiteral("avro")
        ));
    final String sql = SqlFormatter.formatSql(createStream);
    assertThat("literal escaping failure", sql, containsString("`GROUP` STRING"));
    assertThat("not literal escaping failure", sql, containsString("NOLIT STRING"));
    assertThat("lowercase literal escaping failure", sql, containsString("`Having` STRING"));
    final List<PreparedStatement<?>> statements = KsqlParserTestUtil.buildAst(sql,
        MetaStoreFixture.getNewMetaStore(mock(FunctionRegistry.class)));
    assertFalse("formatted sql parsing error", statements.isEmpty());
  }

  @Test
  public void shouldFormatCreateWithEmptySchema() {
    final CreateStream createStream = new CreateStream(
        QualifiedName.of("TEST"),
        Collections.emptyList(),
        false,
        ImmutableMap.of(
            DdlConfig.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("topic_test"),
            DdlConfig.VALUE_FORMAT_PROPERTY, new StringLiteral("avro")
        ));
    final String sql = SqlFormatter.formatSql(createStream);
    final String expectedSql = "CREATE STREAM TEST  WITH (VALUE_FORMAT='avro', KAFKA_TOPIC='topic_test');";
    assertThat(sql, equalTo(expectedSql));
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
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    assertThat(SqlFormatter.formatSql(statement), equalTo("CREATE STREAM S AS SELECT FETCH_FIELD_FROM_STRUCT(A.ADDRESS, 'CITY') \"ADDRESS__CITY\"\n"
        + "FROM ADDRESS A"));
  }

  @Test
  public void shouldFormatSelectStarCorrectly() {
    final String statementString = "CREATE STREAM S AS SELECT * FROM address;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    assertThat(SqlFormatter.formatSql(statement),
        equalTo("CREATE STREAM S AS SELECT *\n"
            + "FROM ADDRESS ADDRESS"));
  }

  @Test
  public void shouldFormatSelectStarCorrectlyWithOtherFields() {
    final String statementString = "CREATE STREAM S AS SELECT *, address AS city FROM address;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    assertThat(SqlFormatter.formatSql(statement),
        equalTo("CREATE STREAM S AS SELECT\n"
            + "  *\n"
            + ", ADDRESS.ADDRESS \"CITY\"\n"
            + "FROM ADDRESS ADDRESS"));
  }

  @Test
  public void shouldFormatSelectStarCorrectlyWithJoin() {
    final String statementString = "CREATE STREAM S AS SELECT address.*, itemid.* "
        + "FROM address INNER JOIN itemid ON address.address = itemid.address->address;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    assertThat(SqlFormatter.formatSql(statement),
        equalTo("CREATE STREAM S AS SELECT\n"
            + "  ADDRESS.*\n"
            + ", ITEMID.*\n"
            + "FROM ADDRESS ADDRESS\n"
            + "INNER JOIN ITEMID ITEMID ON ((ADDRESS.ADDRESS = ITEMID.ADDRESS->ADDRESS))"));
  }

  @Test
  public void shouldFormatSelectStarCorrectlyWithJoinOneSidedStar() {
    final String statementString = "CREATE STREAM S AS SELECT address.*, itemid.ordertime "
        + "FROM address INNER JOIN itemid ON address.address = itemid.address->address;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    assertThat(SqlFormatter.formatSql(statement),
        equalTo("CREATE STREAM S AS SELECT\n"
            + "  ADDRESS.*\n"
            + ", ITEMID.ORDERTIME \"ORDERTIME\"\n"
            + "FROM ADDRESS ADDRESS\n"
            + "INNER JOIN ITEMID ITEMID ON ((ADDRESS.ADDRESS = ITEMID.ADDRESS->ADDRESS))"));
  }

  @Test
  public void shouldFormatSelectCorrectlyWithDuplicateFields() {
    final String statementString = "CREATE STREAM S AS SELECT address AS one, address AS two FROM address;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();
    assertThat(SqlFormatter.formatSql(statement),
        equalTo("CREATE STREAM S AS SELECT\n"
            + "  ADDRESS.ADDRESS \"ONE\"\n"
            + ", ADDRESS.ADDRESS \"TWO\"\n"
            + "FROM ADDRESS ADDRESS"));
  }

  @Test
  public void shouldFormatCsasWithClause() {
    final String statementString = "CREATE STREAM S WITH(partitions=4) AS SELECT * FROM address;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, startsWith("CREATE STREAM S WITH (PARTITIONS = 4) AS SELECT"));
  }

  @Test
  public void shouldFormatCtasWithClause() {
    final String statementString = "CREATE TABLE S WITH(partitions=4) AS SELECT * FROM address;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, startsWith("CREATE TABLE S WITH (PARTITIONS = 4) AS SELECT"));
  }

  @Test
  public void shouldFormatCsasPartitionBy() {
    final String statementString = "CREATE STREAM S AS SELECT * FROM ADDRESS PARTITION BY ADDRESS;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, startsWith("CREATE STREAM S AS SELECT *\n"
        + "FROM ADDRESS ADDRESS\n"
        + "PARTITION BY ADDRESS"));
  }

  @Test
  public void shouldFormatInsertIntoPartitionBy() {
    final String statementString = "INSERT INTO ADDRESS SELECT * FROM ADDRESS PARTITION BY ADDRESS;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, startsWith("INSERT INTO ADDRESS SELECT *\n"
        + "FROM ADDRESS ADDRESS\n"
        + "PARTITION BY ADDRESS"));
  }

  @Test
  public void shouldFormatExplainQuery() {
    final String statementString = "EXPLAIN foo;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, is("EXPLAIN \nfoo"));
  }

  @Test
  public void shouldFormatExplainStatement() {
    final String statementString = "EXPLAIN SELECT * FROM ADDRESS;";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

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
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, is("INSERT INTO ADDRESS (NUMBER, STREET, CITY) VALUES (2, 'high', 'palo alto')"));
  }

  @Test
  public void shouldFormatInsertValuesNoSchema() {
    final String statementString = "INSERT INTO ADDRESS VALUES (2);";
    final Statement statement = KsqlParserTestUtil.buildSingleAst(statementString, metaStore)
        .getStatement();

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
}

