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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.Literal;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.properties.with.CreateSourceProperties;
import io.confluent.ksql.parser.tree.AliasedRelation;
import io.confluent.ksql.parser.tree.ColumnConstraints;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.DescribeStreams;
import io.confluent.ksql.parser.tree.DescribeTables;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.DropTable;
import io.confluent.ksql.parser.tree.Join;
import io.confluent.ksql.parser.tree.JoinCriteria;
import io.confluent.ksql.parser.tree.JoinOn;
import io.confluent.ksql.parser.tree.JoinedSource;
import io.confluent.ksql.parser.tree.ListConnectorPlugins;
import io.confluent.ksql.parser.tree.ListStreams;
import io.confluent.ksql.parser.tree.ListTables;
import io.confluent.ksql.parser.tree.ListVariables;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.Table;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.TableElements;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.parser.tree.WithinExpression;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;

public class SqlFormatterTest {

  private AliasedRelation leftAlias;
  private AliasedRelation rightAlias;
  private AliasedRelation right2Alias;
  private JoinCriteria criteria;
  private JoinCriteria criteria2;

  private MutableMetaStore metaStore;

  private static final SourceName TEST = SourceName.of("TEST");
  private static final SourceName SOMETHING = SourceName.of("SOMETHING");

  private static final ColumnConstraints PRIMARY_KEY_CONSTRAINT =
      new ColumnConstraints.Builder().primaryKey().build();

  private static final ColumnConstraints KEY_CONSTRAINT =
      new ColumnConstraints.Builder().key().build();

  private static final ColumnConstraints HEADERS_CONSTRAINT =
      new ColumnConstraints.Builder().headers().build();

  private static final ColumnConstraints HEADER_KEY1_CONSTRAINT =
      new ColumnConstraints.Builder().header("k1").build();

  private static final ColumnConstraints HEADER_KEY2_CONSTRAINT =
      new ColumnConstraints.Builder().header("k2").build();

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
      .keyColumn(ColumnName.of("K0"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("ITEMID"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("NAME"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("CATEGORY"), categorySchema)
      .build();

  private static final LogicalSchema TABLE_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("K0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("TABLE"), SqlTypes.STRING)
      .build();

  private static final LogicalSchema ORDERS_SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("K1"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("ORDERTIME"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("ORDERID"), SqlTypes.BIGINT)
      .valueColumn(ColumnName.of("ITEMID"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("ITEMINFO"), ITEM_INFO_STRUCT)
      .valueColumn(ColumnName.of("ORDERUNITS"), SqlTypes.INTEGER)
      .valueColumn(ColumnName.of("ARRAYCOL"), SqlTypes.array(SqlTypes.DOUBLE))
      .valueColumn(ColumnName.of("MAPCOL"), SqlTypes.map(SqlTypes.STRING, SqlTypes.DOUBLE))
      .valueColumn(ColumnName.of("ADDRESS"), addressSchema)
      .valueColumn(ColumnName.of("SIZE"), SqlTypes.INTEGER) // Reserved word
      .build();

  private static final CreateSourceProperties SOME_WITH_PROPS = CreateSourceProperties.from(
      ImmutableMap.of(
          CommonCreateConfigs.VALUE_FORMAT_PROPERTY, new StringLiteral("JSON"),
          CommonCreateConfigs.KAFKA_TOPIC_NAME_PROPERTY, new StringLiteral("topic_test"))
  );

  private static final TableElements ELEMENTS_WITH_SINGLE_HEADERS = TableElements.of(
      new TableElement(ColumnName.of("h1"), new Type(SqlTypes.STRING), HEADER_KEY1_CONSTRAINT),
      new TableElement(ColumnName.of("h2"), new Type(SqlTypes.STRING), HEADER_KEY2_CONSTRAINT),
      new TableElement(ColumnName.of("Foo"), new Type(SqlTypes.STRING))
  );

  private static final TableElements ELEMENTS_WITH_HEADER = TableElements.of(
      new TableElement(ColumnName.of("k3"), new Type(SqlTypes.STRING), HEADERS_CONSTRAINT),
      new TableElement(ColumnName.of("Foo"), new Type(SqlTypes.STRING))
  );
  
  private static final TableElements ELEMENTS_WITH_KEY = TableElements.of(
      new TableElement(ColumnName.of("k3"), new Type(SqlTypes.STRING), KEY_CONSTRAINT),
      new TableElement(ColumnName.of("Foo"), new Type(SqlTypes.STRING))
  );

  private static final TableElements ELEMENTS_WITH_PRIMARY_KEY = TableElements.of(
      new TableElement(ColumnName.of("k3"), new Type(SqlTypes.STRING), PRIMARY_KEY_CONSTRAINT),
      new TableElement(ColumnName.of("Foo"), new Type(SqlTypes.STRING))
  );

  private static final TableElements ELEMENTS_WITHOUT_KEY = TableElements.of(
      new TableElement(ColumnName.of("Foo"), new Type(SqlTypes.STRING)),
      new TableElement(ColumnName.of("Bar"), new Type(SqlTypes.STRING))
  );

  @Before
  public void setUp() {
    final Table left = new Table(SourceName.of("left"));
    final Table right = new Table(SourceName.of("right"));
    final Table right2 = new Table(SourceName.of("right2"));

    leftAlias = new AliasedRelation(left, SourceName.of("L"));
    rightAlias = new AliasedRelation(right, SourceName.of("R"));
    right2Alias = new AliasedRelation(right2, SourceName.of("R2"));

    criteria = new JoinOn(new ComparisonExpression(ComparisonExpression.Type.EQUAL,
        new StringLiteral("left.col0"),
        new StringLiteral("right.col0")));
    criteria2 = new JoinOn(new ComparisonExpression(ComparisonExpression.Type.EQUAL,
        new StringLiteral("left.col0"),
        new StringLiteral("right2.col0")));

    metaStore = MetaStoreFixture.getNewMetaStore(mock(FunctionRegistry.class));

    final KsqlTopic ksqlTopicOrders = new KsqlTopic(
        "orders_topic",
        KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name()), SerdeFeatures.of()),
        ValueFormat.of(FormatInfo.of(FormatFactory.JSON.name()), SerdeFeatures.of())
    );

    final KsqlStream<?> ksqlStreamOrders = new KsqlStream<>(
        "sqlexpression",
        SourceName.of("ADDRESS"),
        ORDERS_SCHEMA,
        Optional.empty(),
        false,
        ksqlTopicOrders,
        false
    );

    metaStore.putSource(ksqlStreamOrders, false);

    final KsqlTopic ksqlTopicItems = new KsqlTopic(
        "item_topic",
        KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name()), SerdeFeatures.of()),
        ValueFormat.of(FormatInfo.of(FormatFactory.JSON.name()), SerdeFeatures.of())
    );
    final KsqlTable<String> ksqlTableOrders = new KsqlTable<>(
        "sqlexpression",
        SourceName.of("ITEMID"),
        ITEM_INFO_SCHEMA,
        Optional.empty(),
        false,
        ksqlTopicItems,
        false
    );

    metaStore.putSource(ksqlTableOrders, false);

    final KsqlTable<String> ksqlTableTable = new KsqlTable<>(
        "sqlexpression",
        SourceName.of("TABLE"),
        TABLE_SCHEMA,
        Optional.empty(),
        false,
        ksqlTopicItems,
        false
    );

    metaStore.putSource(ksqlTableTable, false);
  }

  @Test
  public void shouldFormatCreateStreamStatementWithHeader() {
    // Given:
    final CreateStream createStream = new CreateStream(
        TEST,
        ELEMENTS_WITH_HEADER,
        false,
        false,
        SOME_WITH_PROPS,
        false);

    // When:
    final String sql = SqlFormatter.formatSql(createStream);

    // Then:
    assertThat(sql, is("CREATE STREAM TEST (`k3` STRING HEADERS, `Foo` STRING) "
        + "WITH (KAFKA_TOPIC='topic_test', VALUE_FORMAT='JSON');"));
  }

  @Test
  public void shouldFormatCreateStreamStatementWithSingleHeaderKeys() {
    // Given:
    final CreateStream createStream = new CreateStream(
        TEST,
        ELEMENTS_WITH_SINGLE_HEADERS,
        false,
        false,
        SOME_WITH_PROPS,
        false);

    // When:
    final String sql = SqlFormatter.formatSql(createStream);

    // Then:
    assertThat(sql, is("CREATE STREAM TEST (`h1` STRING HEADER('k1'), `h2` STRING HEADER('k2'), `Foo` STRING) "
        + "WITH (KAFKA_TOPIC='topic_test', VALUE_FORMAT='JSON');"));
  }

  @Test
  public void shouldFormatCreateStreamStatementWithExplicitKey() {
    // Given:
    final CreateStream createStream = new CreateStream(
        TEST,
        ELEMENTS_WITH_KEY,
        false,
        false,
        SOME_WITH_PROPS,
        false);

    // When:
    final String sql = SqlFormatter.formatSql(createStream);

    // Then:
    assertThat(sql, is("CREATE STREAM TEST (`k3` STRING KEY, `Foo` STRING) "
        + "WITH (KAFKA_TOPIC='topic_test', VALUE_FORMAT='JSON');"));
  }

  @Test
  public void shouldFormatCreateStreamStatementWithImplicitKey() {
    // Given:
    final CreateStream createStream = new CreateStream(
        TEST,
        ELEMENTS_WITHOUT_KEY,
        false,
        false,
        SOME_WITH_PROPS,
        false);

    // When:
    final String sql = SqlFormatter.formatSql(createStream);

    // Then:
    assertThat(sql, is("CREATE STREAM TEST (`Foo` STRING, `Bar` STRING) "
        + "WITH (KAFKA_TOPIC='topic_test', VALUE_FORMAT='JSON');"));
  }

  @Test
  public void shouldFormatCreateOrReplaceStreamStatement() {
    // Given:
    final CreateSourceProperties props = CreateSourceProperties.from(
        new ImmutableMap.Builder<String, Literal>()
            .putAll(SOME_WITH_PROPS.copyOfOriginalLiterals())
            .build()
    );
    final CreateStream createTable = new CreateStream(
        TEST,
        ELEMENTS_WITHOUT_KEY,
        true,
        false,
        props,
        false);

    // When:
    final String sql = SqlFormatter.formatSql(createTable);

    // Then:
    assertThat(sql, is("CREATE OR REPLACE STREAM TEST (`Foo` STRING, `Bar` STRING) "
        + "WITH (KAFKA_TOPIC='topic_test', VALUE_FORMAT='JSON');"));
  }

  @Test
  public void shouldFormatCreateSourceTableStatement() {
    // Given:
    final CreateSourceProperties props = CreateSourceProperties.from(
        new ImmutableMap.Builder<String, Literal>()
            .putAll(SOME_WITH_PROPS.copyOfOriginalLiterals())
            .build()
    );
    final CreateTable createTable = new CreateTable(
        TEST,
        ELEMENTS_WITH_PRIMARY_KEY,
        false,
        false,
        props,
        true);

    // When:
    final String sql = SqlFormatter.formatSql(createTable);

    // Then:
    assertThat(sql, is("CREATE SOURCE TABLE TEST (`k3` STRING PRIMARY KEY, `Foo` STRING) "
        + "WITH (KAFKA_TOPIC='topic_test', VALUE_FORMAT='JSON');"));
  }

  @Test
  public void shouldFormatCreateSourceStreamStatement() {
    // Given:
    final CreateSourceProperties props = CreateSourceProperties.from(
        new ImmutableMap.Builder<String, Literal>()
            .putAll(SOME_WITH_PROPS.copyOfOriginalLiterals())
            .build()
    );
    final CreateStream createStream = new CreateStream(
        TEST,
        ELEMENTS_WITH_KEY,
        false,
        false,
        props,
        true);

    // When:
    final String sql = SqlFormatter.formatSql(createStream);

    // Then:
    assertThat(sql, is("CREATE SOURCE STREAM TEST (`k3` STRING KEY, `Foo` STRING) "
        + "WITH (KAFKA_TOPIC='topic_test', VALUE_FORMAT='JSON');"));
  }

  @Test
  public void shouldFormatCreateOrReplaceTableStatement() {
    // Given:
    final CreateSourceProperties props = CreateSourceProperties.from(
        new ImmutableMap.Builder<String, Literal>()
            .putAll(SOME_WITH_PROPS.copyOfOriginalLiterals())
            .build()
    );
    final CreateTable createTable = new CreateTable(
        TEST,
        ELEMENTS_WITH_PRIMARY_KEY,
        true,
        false,
        props,
        false);

    // When:
    final String sql = SqlFormatter.formatSql(createTable);

    // Then:
    assertThat(sql, is("CREATE OR REPLACE TABLE TEST (`k3` STRING PRIMARY KEY, `Foo` STRING) "
        + "WITH (KAFKA_TOPIC='topic_test', VALUE_FORMAT='JSON');"));
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
        ELEMENTS_WITH_PRIMARY_KEY,
        false,
        false,
        props,
        false);

    // When:
    final String sql = SqlFormatter.formatSql(createTable);

    // Then:
    assertThat(sql, is("CREATE TABLE TEST (`k3` STRING PRIMARY KEY, `Foo` STRING) "
        + "WITH (KAFKA_TOPIC='topic_test', "
        + "TIMESTAMP='Foo', TIMESTAMP_FORMAT='%s', VALUE_FORMAT='JSON');"));
  }

  @Test
  public void shouldFormatCreateTableStatementWithExplicitKey() {
    // Given:
    final CreateTable createTable = new CreateTable(
        TEST,
        ELEMENTS_WITH_PRIMARY_KEY,
        false,
        false,
        SOME_WITH_PROPS,
        false);

    // When:
    final String sql = SqlFormatter.formatSql(createTable);

    // Then:
    assertThat(sql, is("CREATE TABLE TEST (`k3` STRING PRIMARY KEY, `Foo` STRING) "
        + "WITH (KAFKA_TOPIC='topic_test', VALUE_FORMAT='JSON');"));
  }

  @Test
  public void shouldFormatCreateTableStatementWithImplicitKey() {
    // Given:
    final CreateTable createTable = new CreateTable(
        TEST,
        ELEMENTS_WITHOUT_KEY,
        false,
        false,
        SOME_WITH_PROPS,
        false);

    // When:
    final String sql = SqlFormatter.formatSql(createTable);

    // Then:
    assertThat(sql, is("CREATE TABLE TEST (`Foo` STRING, `Bar` STRING) "
        + "WITH (KAFKA_TOPIC='topic_test', VALUE_FORMAT='JSON');"));
  }

  @Test
  public void shouldFormatTableElementsNamedAfterReservedWords() {
    // Given:
    final TableElements tableElements = TableElements.of(
        new TableElement(ColumnName.of("GROUP"), new Type(SqlTypes.STRING)),
        new TableElement(ColumnName.of("Having"), new Type(SqlTypes.STRING))
    );

    final CreateStream createStream = new CreateStream(
        TEST,
        tableElements,
        false,
        false,
        SOME_WITH_PROPS,
        false);

    // When:
    final String sql = SqlFormatter.formatSql(createStream);

    // Then:
    assertThat("literal escaping failure", sql, containsString("`GROUP` STRING"));
    assertThat("lowercase literal escaping failure", sql, containsString("`Having` STRING"));

    assertValidSql(sql);
  }

  @Test
  public void shouldFormatLeftJoinWithWithin() {
    final Join join = new Join(leftAlias, ImmutableList.of(new JoinedSource(
        Optional.empty(),
        rightAlias,
        JoinedSource.Type.LEFT,
        criteria,
        Optional.of(new WithinExpression(10, TimeUnit.SECONDS)))));

    final String expected = "`left` L\nLEFT OUTER JOIN `right` R WITHIN 10 SECONDS ON "
        + "(('left.col0' = 'right.col0'))";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }

  @Test
  public void shouldFormatRightJoinWithoutJoinWindow() {
    final Join join = new Join(leftAlias, ImmutableList.of(new JoinedSource(
        Optional.empty(),
        rightAlias,
        JoinedSource.Type.RIGHT,
        criteria,
        Optional.empty())));

    final String result = SqlFormatter.formatSql(join);
    final String expected = "`left` L\nRIGHT OUTER JOIN `right` R ON (('left.col0' = 'right.col0'))";
    assertEquals(expected, result);
  }

  @Test
  public void shouldFormatRightJoinWithWithin() {
    final Join join = new Join(leftAlias, ImmutableList.of(new JoinedSource(
        Optional.empty(),
        rightAlias,
        JoinedSource.Type.RIGHT,
        criteria,
        Optional.of(new WithinExpression(10, TimeUnit.SECONDS)))));

    final String expected = "`left` L\nRIGHT OUTER JOIN `right` R WITHIN 10 SECONDS ON "
        + "(('left.col0' = 'right.col0'))";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }

  @Test
  public void shouldFormatLeftJoinWithoutJoinWindow() {
    final Join join = new Join(leftAlias, ImmutableList.of(new JoinedSource(
        Optional.empty(),
        rightAlias,
        JoinedSource.Type.LEFT,
        criteria,
        Optional.empty())));

    final String result = SqlFormatter.formatSql(join);
    final String expected = "`left` L\nLEFT OUTER JOIN `right` R ON (('left.col0' = 'right.col0'))";
    assertEquals(expected, result);
  }

  @Test
  public void shouldFormatInnerJoin() {
    final Join join = new Join(leftAlias, ImmutableList.of(new JoinedSource(
        Optional.empty(),
        rightAlias,
        JoinedSource.Type.INNER,
        criteria,
        Optional.of(new WithinExpression(10, TimeUnit.SECONDS)))));

    final String expected = "`left` L\nINNER JOIN `right` R WITHIN 10 SECONDS ON "
        + "(('left.col0' = 'right.col0'))";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }

  @Test
  public void shouldFormatInnerJoinWithoutJoinWindow() {
    final Join join = new Join(leftAlias, ImmutableList.of(new JoinedSource(
        Optional.empty(),
        rightAlias,
        JoinedSource.Type.INNER,
        criteria,
        Optional.empty())));

    final String expected = "`left` L\nINNER JOIN `right` R ON (('left.col0' = 'right.col0'))";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }


  @Test
  public void shouldFormatOuterJoin() {
    final Join join = new Join(leftAlias, ImmutableList.of(new JoinedSource(
        Optional.empty(),
        rightAlias,
        JoinedSource.Type.OUTER,
        criteria,
        Optional.of(new WithinExpression(10, TimeUnit.SECONDS)))));

    final String expected = "`left` L\nFULL OUTER JOIN `right` R WITHIN 10 SECONDS ON"
        + " (('left.col0' = 'right.col0'))";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }


  @Test
  public void shouldFormatOuterJoinWithoutJoinWindow() {
    final Join join = new Join(leftAlias, ImmutableList.of(new JoinedSource(
        Optional.empty(),
        rightAlias,
        JoinedSource.Type.OUTER,
        criteria,
        Optional.empty())));

    final String expected = "`left` L\nFULL OUTER JOIN `right` R ON (('left.col0' = 'right.col0'))";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }

  @Test
  public void shouldFormatMultipleLeftJoinWithoutWindow() {
    final List<JoinedSource> rights = ImmutableList.of(
        new JoinedSource(Optional.empty(), rightAlias, JoinedSource.Type.LEFT, criteria, Optional.empty()),
        new JoinedSource(Optional.empty(), right2Alias, JoinedSource.Type.LEFT, criteria2, Optional.empty())
    );

    final Join join = new Join(leftAlias, rights);

    final String expected = "`left` L"
        + "\nLEFT OUTER JOIN `right` R ON (('left.col0' = 'right.col0'))"
        + "\nLEFT OUTER JOIN `right2` R2 ON (('left.col0' = 'right2.col0'))";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }

  @Test
  public void shouldFormatMultipleLeftJoinWithWithin() {
    final List<JoinedSource> rights = ImmutableList.of(
        new JoinedSource(Optional.empty(), rightAlias, JoinedSource.Type.LEFT, criteria, Optional.of(new WithinExpression(10, TimeUnit.SECONDS))),
        new JoinedSource(Optional.empty(), right2Alias, JoinedSource.Type.LEFT, criteria2, Optional.of(new WithinExpression(10, TimeUnit.SECONDS)))
    );

    final Join join = new Join(leftAlias, rights);

    final String expected = "`left` L"
        + "\nLEFT OUTER JOIN `right` R WITHIN 10 SECONDS ON (('left.col0' = 'right.col0'))"
        + "\nLEFT OUTER JOIN `right2` R2 WITHIN 10 SECONDS ON (('left.col0' = 'right2.col0'))";
    assertEquals(expected, SqlFormatter.formatSql(join));
  }

  @Test
  public void shouldFormatMultipleJoinsWithDifferentTypes() {
    final List<JoinedSource> rights = ImmutableList.of(
        new JoinedSource(Optional.empty(), rightAlias, JoinedSource.Type.LEFT, criteria, Optional.of(new WithinExpression(10, TimeUnit.SECONDS))),
        new JoinedSource(Optional.empty(), right2Alias, JoinedSource.Type.INNER, criteria2, Optional.of(new WithinExpression(10, TimeUnit.SECONDS)))
    );

    final Join join = new Join(leftAlias, rights);

    final String expected = "`left` L"
        + "\nLEFT OUTER JOIN `right` R WITHIN 10 SECONDS ON (('left.col0' = 'right.col0'))"
        + "\nINNER JOIN `right2` R2 WITHIN 10 SECONDS ON (('left.col0' = 'right2.col0'))";
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
  public void shouldFormatReplaceSelectQueryCorrectly() {
    final String statementString =
        "CREATE OR REPLACE STREAM S AS SELECT a.address->city FROM address a;";
    final Statement statement = parseSingle(statementString);
    assertThat(SqlFormatter.formatSql(statement), equalTo("CREATE OR REPLACE STREAM S AS SELECT A.ADDRESS->CITY\n"
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
  public void shouldFormatDefineStatement() {
    final String statementString = "DEFINE _topic='t1';";
    final Statement statement = parseSingle(statementString);

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, is("DEFINE _topic='t1'"));
  }

  @Test
  public void shouldFormatUndefineStatement() {
    final String statementString = "UNDEFINE _topic;";
    final Statement statement = parseSingle(statementString);

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, is("UNDEFINE _topic"));
  }

  @Test
  public void shouldFormatExplainQuery() {
    final String statementString = "EXPLAIN foo;";
    final Statement statement = parseSingle(statementString);

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, is("EXPLAIN \nFOO"));
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
  public void shouldFormatShowConnectorPlugins() {
    // Given:
    final ListConnectorPlugins listConnectorPlugins = new ListConnectorPlugins(Optional.empty());

    // When:
    final String formatted = SqlFormatter.formatSql(listConnectorPlugins);

    // Then:
    assertThat(formatted, is("SHOW CONNECTOR PLUGINS"));
  }

  @Test
  public void shouldFormatDescribeTables() {
    // Given:
    final DescribeTables describeTables = new DescribeTables(Optional.empty(), false);

    // When:
    final String formatted = SqlFormatter.formatSql(describeTables);

    // Then:
    assertThat(formatted, is("DESCRIBE TABLES"));
  }

  @Test
  public void shouldFormatShowVariables() {
    // Given:
    final ListVariables listVariables = new ListVariables(Optional.empty());

    // When:
    final String formatted = SqlFormatter.formatSql(listVariables);

    // Then:
    assertThat(formatted, is("SHOW VARIABLES"));
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
  public void shouldFormatDescribeStreams() {
    // Given:
    final DescribeStreams describeStreams = new DescribeStreams(Optional.empty(), false);

    // When:
    final String formatted = SqlFormatter.formatSql(describeStreams);

    // Then:
    assertThat(formatted, is("DESCRIBE STREAMS"));
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
  public void shouldFormatTumblingWindowWithRetention() {
    // Given:
    final String statementString = "CREATE STREAM S AS SELECT ITEMID, COUNT(*) FROM ORDERS WINDOW TUMBLING (SIZE 7 DAYS, RETENTION 14 DAYS) GROUP BY ITEMID;";
    final Statement statement = parseSingle(statementString);

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, is("CREATE STREAM S AS SELECT\n"
        + "  ITEMID,\n"
        + "  COUNT(*)\n"
        + "FROM ORDERS ORDERS\n"
        + "WINDOW TUMBLING ( SIZE 7 DAYS , RETENTION 14 DAYS ) \n"
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
  public void shouldFormatHoppingWindowWithGracePeriod() {
    // Given:
    final String statementString = "CREATE STREAM S AS SELECT ITEMID, COUNT(*) FROM ORDERS WINDOW HOPPING (SIZE 20 SECONDS, ADVANCE BY 5 SECONDS, GRACE PERIOD 2 HOURS) GROUP BY ITEMID;";
    final Statement statement = parseSingle(statementString);

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, is("CREATE STREAM S AS SELECT\n"
        + "  ITEMID,\n"
        + "  COUNT(*)\n"
        + "FROM ORDERS ORDERS\n"
        + "WINDOW HOPPING ( SIZE 20 SECONDS , ADVANCE BY 5 SECONDS , GRACE PERIOD 2 HOURS ) \n"
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
  public void shouldFormatSessionWindowWithRetentionAndGracePeriod() {
    // Given:
    final Statement statement = parseSingle(
        "CREATE STREAM S AS SELECT ITEMID, COUNT(*) FROM ORDERS WINDOW SESSION (15 MINUTES, RETENTION 2 DAYS, GRACE PERIOD 1 HOUR) GROUP BY ITEMID;");

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, is("CREATE STREAM S AS SELECT\n"
        + "  ITEMID,\n"
        + "  COUNT(*)\n"
        + "FROM ORDERS ORDERS\n"
        + "WINDOW SESSION ( 15 MINUTES , RETENTION 2 DAYS , GRACE PERIOD 1 HOURS ) \n"
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

  @Test
  public void shouldFormatSuppression() {
    // Given:
    final String statementString = "CREATE STREAM S AS SELECT ITEMID, COUNT(*) FROM ORDERS WINDOW TUMBLING (SIZE 7 DAYS, GRACE PERIOD 1 DAY) GROUP BY ITEMID EMIT FINAL;";
    final Statement statement = parseSingle(statementString);

    final String result = SqlFormatter.formatSql(statement);

    assertThat(result, is("CREATE STREAM S AS SELECT\n"
        + "  ITEMID,\n"
        + "  COUNT(*)\n"
        + "FROM ORDERS ORDERS\n"
        + "WINDOW TUMBLING ( SIZE 7 DAYS , GRACE PERIOD 1 DAYS ) \n"
        + "GROUP BY ITEMID\n"
        + "EMIT FINAL"));
  }

  @Test
  public void shouldFormatAlterStatement() {
    // Given:
    final String statementString = "ALTER STREAM FOO ADD COLUMN A STRING, ADD COLUMN B INT;";
    final Statement statement = parseSingle(statementString);

    // When:
    final String result = SqlFormatter.formatSql(statement);

    // Then:
    assertThat(result, is("ALTER STREAM FOO\n"
        + "ADD COLUMN A STRING,\n"
        + "ADD COLUMN B INTEGER;"));
  }

  private Statement parseSingle(final String statementString) {
    return KsqlParserTestUtil.buildSingleAst(statementString, metaStore).getStatement();
  }

  private void assertValidSql(final String sql) {
    // Will throw if invalid
    KsqlParserTestUtil.buildAst(sql, metaStore);
  }
}
