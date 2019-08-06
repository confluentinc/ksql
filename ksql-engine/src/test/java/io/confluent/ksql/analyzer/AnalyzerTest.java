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

package io.confluent.ksql.analyzer;

import static io.confluent.ksql.testutils.AnalysisTestUtil.analyzeQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.confluent.ksql.analyzer.Analysis.Into;
import io.confluent.ksql.analyzer.Analysis.JoinInfo;
import io.confluent.ksql.analyzer.Analyzer.SerdeOptionsSupplier;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.parser.ExpressionFormatter;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.KsqlParserTestUtil;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import io.confluent.ksql.parser.tree.BooleanLiteral;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.Literal;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Sink;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.planner.plan.JoinNode.JoinType;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("OptionalGetWithoutIsPresent")
@RunWith(MockitoJUnitRunner.class)
public class AnalyzerTest {

  private static final Set<SerdeOption> DEFAULT_SERDE_OPTIONS = SerdeOption.none();

  private MutableMetaStore jsonMetaStore;
  private MutableMetaStore avroMetaStore;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private SerdeOptionsSupplier serdeOptiponsSupplier;
  @Mock
  private Sink sink;

  private Query query;
  private Analyzer analyzer;
  private Optional<Format> sinkFormat = Optional.empty();
  private Optional<Boolean> sinkWrapSingleValues = Optional.empty();

  @Before
  public void init() {
    jsonMetaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
    avroMetaStore = MetaStoreFixture.getNewMetaStore(
        new InternalFunctionRegistry(),
        ValueFormat.of(FormatInfo.of(Format.AVRO))
    );

    analyzer = new Analyzer(
        jsonMetaStore,
        "",
        DEFAULT_SERDE_OPTIONS,
        serdeOptiponsSupplier
    );

    when(sink.getName()).thenReturn("TEST0");
    when(sink.getProperties()).thenReturn(CreateSourceAsProperties.none());

    query = parseSingle("Select COL0, COL1 from TEST1;");

    registerKafkaSource();
  }

  @Test
  public void testSimpleQueryAnalysis() {
    final String simpleQuery = "SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final Analysis analysis = analyzeQuery(simpleQuery, jsonMetaStore);
    Assert.assertNotNull("INTO is null", analysis.getInto());
    Assert.assertNotNull("SELECT is null", analysis.getSelectExpressions());
    Assert.assertNotNull("SELECT alias is null", analysis.getSelectExpressionAlias());
    Assert.assertTrue("FROM was not analyzed correctly.",
        analysis.getFromDataSources().get(0).getDataSource().getName()
                          .equalsIgnoreCase("test1"));
    Assert.assertEquals(analysis.getSelectExpressions().size(),
        analysis.getSelectExpressionAlias().size());
    final String
        sqlStr =
        ExpressionFormatter.formatExpression(analysis.getWhereExpression()).replace("\n", " ");
    Assert.assertTrue(sqlStr.equalsIgnoreCase("(TEST1.COL0 > 100)"));

    final String
        select1 =
        ExpressionFormatter.formatExpression(analysis.getSelectExpressions().get(0))
            .replace("\n", " ");
    Assert.assertTrue(select1.equalsIgnoreCase("TEST1.COL0"));
    final String
        select2 =
        ExpressionFormatter.formatExpression(analysis.getSelectExpressions().get(1))
            .replace("\n", " ");
    Assert.assertTrue(select2.equalsIgnoreCase("TEST1.COL2"));
    final String
        select3 =
        ExpressionFormatter.formatExpression(analysis.getSelectExpressions().get(2))
            .replace("\n", " ");
    Assert.assertTrue(select3.equalsIgnoreCase("TEST1.COL3"));

    Assert.assertTrue(analysis.getSelectExpressionAlias().get(0).equalsIgnoreCase("COL0"));
    Assert.assertTrue(analysis.getSelectExpressionAlias().get(1).equalsIgnoreCase("COL2"));
    Assert.assertTrue(analysis.getSelectExpressionAlias().get(2).equalsIgnoreCase("COL3"));
  }

  @Test
  public void testSimpleLeftJoinAnalysis() {
    // When:
    final Analysis analysis = analyzeQuery(
        "SELECT t1.col1, t2.col1, t2.col4, col5, t2.col2 "
            + "FROM test1 t1 LEFT JOIN test2 t2 "
            + "ON t1.col1 = t2.col1;", jsonMetaStore);

    // Then:
    assertThat(analysis.getFromDataSources(), hasSize(2));
    assertThat(analysis.getFromDataSources().get(0).getAlias(), is("T1"));
    assertThat(analysis.getFromDataSources().get(1).getAlias(), is("T2"));

    assertThat(analysis.getSelectExpressions().size(),
        is(analysis.getSelectExpressionAlias().size()));

    assertThat(analysis.getJoin(), is(not(Optional.empty())));
    assertThat(analysis.getJoin().get().getLeftJoinField(), is("T1.COL1"));
    assertThat(analysis.getJoin().get().getRightJoinField(), is("T2.COL1"));

    final List<String> selects = analysis.getSelectExpressions().stream()
        .map(ExpressionFormatter::formatExpression)
        .collect(Collectors.toList());

    assertThat(selects, contains("T1.COL1", "T2.COL1", "T2.COL4", "T1.COL5", "T2.COL2"));
    assertThat(analysis.getSelectExpressionAlias(),
        contains("T1_COL1", "T2_COL1", "T2_COL4", "COL5", "T2_COL2"));
  }

  @Test
  public void shouldHandleJoinOnRowKey() {
    // When:
    final Optional<JoinInfo> join = analyzeQuery(
        "SELECT * FROM test1 t1 LEFT JOIN test2 t2 ON t1.ROWKEY = t2.ROWKEY;",
        jsonMetaStore)
        .getJoin();

    // Then:
    assertThat(join, is(not(Optional.empty())));
    assertThat(join.get().getType(), is(JoinType.LEFT));
    assertThat(join.get().getLeftJoinField(), is("T1.ROWKEY"));
    assertThat(join.get().getRightJoinField(), is("T2.ROWKEY"));
  }

  @Test
  public void testBooleanExpressionAnalysis() {
    final String queryStr = "SELECT col0 = 10, col2, col3 > col1 FROM test1;";
    final Analysis analysis = analyzeQuery(queryStr, jsonMetaStore);

    Assert.assertNotNull("INTO is null", analysis.getInto());
    Assert.assertNotNull("SELECT is null", analysis.getSelectExpressions());
    Assert.assertNotNull("SELECT aliacs is null", analysis.getSelectExpressionAlias());
    Assert.assertTrue("FROM was not analyzed correctly.",
        analysis.getFromDataSources().get(0).getDataSource().getName()
                          .equalsIgnoreCase("test1"));

    final String
        select1 =
        ExpressionFormatter.formatExpression(analysis.getSelectExpressions().get(0))
            .replace("\n", " ");
    Assert.assertTrue(select1.equalsIgnoreCase("(TEST1.COL0 = 10)"));
    final String
        select2 =
        ExpressionFormatter.formatExpression(analysis.getSelectExpressions().get(1))
            .replace("\n", " ");
    Assert.assertTrue(select2.equalsIgnoreCase("TEST1.COL2"));
    final String
        select3 =
        ExpressionFormatter.formatExpression(analysis.getSelectExpressions().get(2))
            .replace("\n", " ");
    Assert.assertTrue(select3.equalsIgnoreCase("(TEST1.COL3 > TEST1.COL1)"));
  }

  @Test
  public void testFilterAnalysis() {
    final String queryStr = "SELECT col0 = 10, col2, col3 > col1 FROM test1 WHERE col0 > 20;";
    final Analysis analysis = analyzeQuery(queryStr, jsonMetaStore);

    Assert.assertNotNull("INTO is null", analysis.getInto());
    Assert.assertNotNull("SELECT is null", analysis.getSelectExpressions());
    Assert.assertNotNull("SELECT aliacs is null", analysis.getSelectExpressionAlias());
    Assert.assertTrue("FROM was not analyzed correctly.",
        analysis.getFromDataSources().get(0).getDataSource().getName()
                    .equalsIgnoreCase("test1"));

    final String
            select1 =
        ExpressionFormatter.formatExpression(analysis.getSelectExpressions().get(0))
                    .replace("\n", " ");
    Assert.assertTrue(select1.equalsIgnoreCase("(TEST1.COL0 = 10)"));
    final String
            select2 =
        ExpressionFormatter.formatExpression(analysis.getSelectExpressions().get(1))
                    .replace("\n", " ");
    Assert.assertTrue(select2.equalsIgnoreCase("TEST1.COL2"));
    final String
            select3 =
        ExpressionFormatter.formatExpression(analysis.getSelectExpressions().get(2))
                    .replace("\n", " ");
    Assert.assertTrue(select3.equalsIgnoreCase("(TEST1.COL3 > TEST1.COL1)"));
    Assert.assertTrue("testFilterAnalysis failed.", analysis.getWhereExpression().toString().equalsIgnoreCase("(TEST1.COL0 > 20)"));

  }

  @Test
  public void shouldCreateCorrectSinkKsqlTopic() {
    final String simpleQuery = "CREATE STREAM FOO WITH (KAFKA_TOPIC='TEST_TOPIC1') AS SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final List<Statement> statements = parse(simpleQuery, jsonMetaStore);
    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statements.get(0);
    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(jsonMetaStore, "", DEFAULT_SERDE_OPTIONS);
    final Analysis analysis = analyzer
        .analyze("sqlExpression", query, Optional.of(createStreamAsSelect.getSink()));

    Assert.assertNotNull("INTO is null", analysis.getInto());
    final Optional<Into> into = analysis.getInto();
    assertThat(into, is(not((Optional.empty()))));
    final KsqlTopic createdKsqlTopic = into.get().getKsqlTopic();
    assertThat(createdKsqlTopic.getKafkaTopicName(), is("TEST_TOPIC1"));
  }

  @Test
  public void shouldUseExplicitNamespaceForAvroSchema() {
    final String simpleQuery = "CREATE STREAM FOO WITH (VALUE_FORMAT='AVRO', VALUE_AVRO_SCHEMA_FULL_NAME='com.custom.schema', KAFKA_TOPIC='TEST_TOPIC1') AS SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final List<Statement> statements = parse(simpleQuery, jsonMetaStore);
    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statements.get(0);
    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(jsonMetaStore, "", DEFAULT_SERDE_OPTIONS);
    final Analysis analysis = analyzer
        .analyze("sqlExpression", query, Optional.of(createStreamAsSelect.getSink()));

    assertThat(analysis.getInto(), is(not(Optional.empty())));
    assertThat(analysis.getInto().get().getKsqlTopic().getValueFormat(),
        is(ValueFormat.of(FormatInfo.of(Format.AVRO, Optional.of("com.custom.schema")))));
  }

  @Test
  public void shouldUseImplicitNamespaceForAvroSchema() {
    final String simpleQuery = "CREATE STREAM FOO WITH (VALUE_FORMAT='AVRO', KAFKA_TOPIC='TEST_TOPIC1') AS SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final List<Statement> statements = parse(simpleQuery, jsonMetaStore);
    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statements.get(0);
    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(jsonMetaStore, "", DEFAULT_SERDE_OPTIONS);
    final Analysis analysis = analyzer
        .analyze("sqlExpression", query, Optional.of(createStreamAsSelect.getSink()));

    assertThat(analysis.getInto(), is(not(Optional.empty())));
    assertThat(analysis.getInto().get().getKsqlTopic().getValueFormat(),
        is(ValueFormat.of(FormatInfo.of(Format.AVRO))));
  }

    @Test
  public void shouldUseExplicitNamespaceWhenFormatIsInheritedForAvro() {
    final String simpleQuery = "create stream s1 with (VALUE_AVRO_SCHEMA_FULL_NAME='org.ac.s1') as select * from test1;";

    final List<Statement> statements = parse(simpleQuery, avroMetaStore);
    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statements.get(0);
    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(avroMetaStore, "", DEFAULT_SERDE_OPTIONS);
    final Analysis analysis = analyzer
          .analyze("sqlExpression", query, Optional.of(createStreamAsSelect.getSink()));

    assertThat(analysis.getInto(), is(not(Optional.empty())));
      assertThat(analysis.getInto().get().getKsqlTopic().getValueFormat(),
          is(ValueFormat.of(FormatInfo.of(Format.AVRO, Optional.of("org.ac.s1")))));
  }

  @Test
  public void shouldNotInheritNamespaceExplicitlySetUpstreamForAvro() {
    final String simpleQuery = "create stream s1 as select * from S0;";

    final MutableMetaStore newAvroMetaStore = avroMetaStore.copy();

    final KsqlTopic ksqlTopic = new KsqlTopic(
        "s0",
        KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
        ValueFormat.of(FormatInfo.of(Format.AVRO, Optional.of("org.ac.s1"))),
        false);

    final LogicalSchema schema = LogicalSchema.builder()
            .valueField("FIELD1", SqlTypes.BIGINT)
            .build();

    final KsqlStream<?> ksqlStream = new KsqlStream<>(
        "create stream s0 with(KAFKA_TOPIC='s0', VALUE_AVRO_SCHEMA_FULL_NAME='org.ac.s1', VALUE_FORMAT='avro');",
        "S0",
        schema,
        SerdeOption.none(),
        KeyField.of("FIELD1", schema.findValueField("FIELD1").get()),
        new MetadataTimestampExtractionPolicy(),
        ksqlTopic
    );

    newAvroMetaStore.putSource(ksqlStream);

    final List<Statement> statements = parse(simpleQuery, newAvroMetaStore);
    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statements.get(0);
    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(newAvroMetaStore, "", DEFAULT_SERDE_OPTIONS);
    final Analysis analysis = analyzer
        .analyze("sqlExpression", query, Optional.of(createStreamAsSelect.getSink()));

    assertThat(analysis.getInto(), is(not(Optional.empty())));
    assertThat(analysis.getInto().get().getKsqlTopic().getValueFormat(),
        is(ValueFormat.of(FormatInfo.of(Format.AVRO))));
  }

  @Test
  public void shouldUseImplicitNamespaceWhenFormatIsInheritedForAvro() {
    final String simpleQuery = "create stream s1 as select * from test1;";

    final List<Statement> statements = parse(simpleQuery, avroMetaStore);
    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statements.get(0);
    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(avroMetaStore, "", DEFAULT_SERDE_OPTIONS);
    final Analysis analysis = analyzer
        .analyze("sqlExpression", query, Optional.of(createStreamAsSelect.getSink()));

    assertThat(analysis.getInto(), is(not(Optional.empty())));
    assertThat(analysis.getInto().get().getKsqlTopic().getValueFormat(),
        is(ValueFormat.of(FormatInfo.of(Format.AVRO))));
  }

  @Test
  public void shouldFailIfExplicitNamespaceIsProvidedForNonAvroTopic() {
    final String simpleQuery = "CREATE STREAM FOO WITH (VALUE_FORMAT='JSON', VALUE_AVRO_SCHEMA_FULL_NAME='com.custom.schema', KAFKA_TOPIC='TEST_TOPIC1') AS SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final List<Statement> statements = parse(simpleQuery, jsonMetaStore);
    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statements.get(0);
    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(jsonMetaStore, "", DEFAULT_SERDE_OPTIONS);

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Full schema name only supported with AVRO format");

    analyzer.analyze("sqlExpression", query, Optional.of(createStreamAsSelect.getSink()));
  }

  @Test
  public void shouldFailIfExplicitNamespaceIsProvidedButEmpty() {
    final CreateStreamAsSelect createStreamAsSelect = parseSingle(
        "CREATE STREAM FOO "
            + "WITH (VALUE_FORMAT='AVRO', VALUE_AVRO_SCHEMA_FULL_NAME='', KAFKA_TOPIC='TEST_TOPIC1') "
            + "AS SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;");

    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(jsonMetaStore, "", DEFAULT_SERDE_OPTIONS);

    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Schema name can not be empty");

    analyzer.analyze("sqlExpression", query, Optional.of(createStreamAsSelect.getSink()));
  }

  @Test
  public void shouldGetSerdeOptions() {
    // Given:
    final Set<SerdeOption> serdeOptions = ImmutableSet.of(SerdeOption.UNWRAP_SINGLE_VALUES);
    when(serdeOptiponsSupplier.build(any(), any(), any(), any())).thenReturn(serdeOptions);

    givenSinkValueFormat(Format.AVRO);
    givenWrapSingleValues(true);

    // When:
    final Analysis result = analyzer.analyze("sql", query, Optional.of(sink));

    // Then:
    verify(serdeOptiponsSupplier).build(
        ImmutableList.of("COL0", "COL1"),
        Format.AVRO,
        Optional.of(true),
        DEFAULT_SERDE_OPTIONS);

    assertThat(result.getSerdeOptions(), is(serdeOptions));
  }

  @Test
  public void shouldExcludeRowTimeAndRowKeyWhenGettingSerdeOptions() {
    // Given:
    final Set<SerdeOption> serdeOptions = ImmutableSet.of(SerdeOption.UNWRAP_SINGLE_VALUES);
    when(serdeOptiponsSupplier.build(any(), any(), any(), any())).thenReturn(serdeOptions);

    query = parseSingle("Select ROWTIME, ROWKEY, ROWTIME AS TIME, ROWKEY AS KEY, COL0, COL1 from TEST1;");

    // When:
    analyzer.analyze("sql", query, Optional.of(sink));

    // Then:
    verify(serdeOptiponsSupplier).build(
        eq(ImmutableList.of("TIME", "KEY", "COL0", "COL1")),
        any(),
        any(),
        any());
  }

  @Test
  public void shouldThrowOnGroupByIfKafkaFormat() {
    // Given:
    query = parseSingle("Select COL0 from KAFKA_SOURCE GROUP BY COL0;");

    givenSinkValueFormat(Format.KAFKA);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Source(s) KAFKA_SOURCE are using the 'KAFKA' value format."
        + " This format does not yet support GROUP BY.");

    // When:
    analyzer.analyze("sql", query, Optional.of(sink));
  }

  @Test
  public void shouldThrowOnJoinIfKafkaFormat() {
    // Given:
    query = parseSingle("Select TEST1.COL0 from TEST1 JOIN KAFKA_SOURCE "
        + "WITHIN 1 SECOND ON "
        + "TEST1.COL0 = KAFKA_SOURCE.COL0;");

    givenSinkValueFormat(Format.KAFKA);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Source(s) KAFKA_SOURCE are using the 'KAFKA' value format."
        + " This format does not yet support JOIN.");

    // When:
    analyzer.analyze("sql", query, Optional.of(sink));
  }

  @SuppressWarnings("unchecked")
  private <T extends Statement> T parseSingle(final String simpleQuery) {
    return (T) Iterables.getOnlyElement(parse(simpleQuery, jsonMetaStore));
  }

  private void givenSinkValueFormat(final Format format) {
    this.sinkFormat = Optional.of(format);
    buildProps();
  }

  private void givenWrapSingleValues(final boolean wrap) {
    this.sinkWrapSingleValues = Optional.of(wrap);
    buildProps();
  }

  private void buildProps() {
    final Map<String, Literal> props = new HashMap<>();
    sinkFormat.ifPresent(f -> props.put("VALUE_FORMAT", new StringLiteral(f.toString())));
    sinkWrapSingleValues.ifPresent(b -> props.put("WRAP_SINGLE_VALUE", new BooleanLiteral(Boolean.toString(b))));

    final CreateSourceAsProperties properties = CreateSourceAsProperties.from(props);

    when(sink.getProperties()).thenReturn(properties);

  }

  private void registerKafkaSource() {
    final Schema schema = SchemaBuilder.struct()
        .field("COL0", Schema.OPTIONAL_INT64_SCHEMA)
        .build();

    final KsqlTopic topic = new KsqlTopic(
        "ks",
        KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
        ValueFormat.of(FormatInfo.of(Format.KAFKA)),
        false);

    final KsqlStream<?> stream = new KsqlStream<>(
        "sqlexpression",
        "KAFKA_SOURCE",
        LogicalSchema.of(schema),
        SerdeOption.none(),
        KeyField.none(),
        new MetadataTimestampExtractionPolicy(),
        topic
    );

    jsonMetaStore.putSource(stream);
  }

  private static List<Statement> parse(final String simpleQuery, final MetaStore metaStore) {
    return KsqlParserTestUtil.buildAst(simpleQuery, metaStore)
        .stream()
        .map(PreparedStatement::getStatement)
        .collect(Collectors.toList());
  }
}
