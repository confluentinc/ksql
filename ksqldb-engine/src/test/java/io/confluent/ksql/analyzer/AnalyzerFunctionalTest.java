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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Sink;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.serde.avro.AvroFormat;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlParserTestUtil;
import io.confluent.ksql.util.MetaStoreFixture;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * DO NOT ADD NEW TESTS TO THIS FILE
 *
 * <p>Instead add new JSON based tests to QueryTranslationTest.
 *
 * <p>This test file is more of a functional test, which is better implemented using QTT.
 */
@SuppressWarnings("OptionalGetWithoutIsPresent")
@RunWith(MockitoJUnitRunner.class)
public class AnalyzerFunctionalTest {

  private static final ColumnName COL0 = ColumnName.of("COL0");
  private static final ColumnName COL1 = ColumnName.of("COL1");
  private static final ColumnName COL2 = ColumnName.of("COL2");

  private MutableMetaStore jsonMetaStore;
  private MutableMetaStore avroMetaStore;

  @Mock
  private Sink sink;

  private Query query;
  private Analyzer analyzer;

  @Before
  public void init() {
    jsonMetaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
    avroMetaStore = MetaStoreFixture.getNewMetaStore(
        new InternalFunctionRegistry(),
        ValueFormat.of(FormatInfo.of(FormatFactory.AVRO.name()))
    );

    analyzer = new Analyzer(jsonMetaStore, "");

    query = parseSingle("Select COL0, COL1 from TEST1;");

    registerKafkaSource();
  }


  @Test
  public void shouldUseExplicitNamespaceForAvroSchema() {
    final String simpleQuery = "CREATE STREAM FOO WITH (VALUE_FORMAT='AVRO', VALUE_AVRO_SCHEMA_FULL_NAME='com.custom.schema', KAFKA_TOPIC='TEST_TOPIC1') AS SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final List<Statement> statements = parse(simpleQuery, jsonMetaStore);
    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statements.get(0);
    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(jsonMetaStore, "");
    final Analysis analysis = analyzer.analyze(query, Optional.of(createStreamAsSelect.getSink()));

    assertThat(analysis.getInto(), is(not(Optional.empty())));
    assertThat(analysis.getInto().get().getKsqlTopic().getValueFormat(),
        is(ValueFormat.of(FormatInfo.of(
            FormatFactory.AVRO.name(), ImmutableMap.of(AvroFormat.FULL_SCHEMA_NAME, "com.custom.schema")))));
  }

  @Test
  public void shouldUseImplicitNamespaceForAvroSchema() {
    final String simpleQuery = "CREATE STREAM FOO WITH (VALUE_FORMAT='AVRO', KAFKA_TOPIC='TEST_TOPIC1') AS SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final List<Statement> statements = parse(simpleQuery, jsonMetaStore);
    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statements.get(0);
    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(jsonMetaStore, "");
    final Analysis analysis = analyzer.analyze(query, Optional.of(createStreamAsSelect.getSink()));

    assertThat(analysis.getInto(), is(not(Optional.empty())));
    assertThat(analysis.getInto().get().getKsqlTopic().getValueFormat(),
        is(ValueFormat.of(FormatInfo.of(FormatFactory.AVRO.name()))));
  }

  @Test
  public void shouldUseExplicitNamespaceWhenFormatIsInheritedForAvro() {
    final String simpleQuery = "create stream s1 with (VALUE_AVRO_SCHEMA_FULL_NAME='org.ac.s1') as select * from test1;";

    final List<Statement> statements = parse(simpleQuery, avroMetaStore);
    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statements.get(0);
    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(avroMetaStore, "");
    final Analysis analysis = analyzer.analyze(query, Optional.of(createStreamAsSelect.getSink()));

    assertThat(analysis.getInto(), is(not(Optional.empty())));
    assertThat(analysis.getInto().get().getKsqlTopic().getValueFormat(),
        is(ValueFormat.of(FormatInfo.of(FormatFactory.AVRO.name(), ImmutableMap
            .of(AvroFormat.FULL_SCHEMA_NAME, "org.ac.s1")))));
  }

  @Test
  public void shouldNotInheritNamespaceExplicitlySetUpstreamForAvro() {
    final String simpleQuery = "create stream s1 as select * from S0;";

    final MutableMetaStore newAvroMetaStore = avroMetaStore.copy();

    final KsqlTopic ksqlTopic = new KsqlTopic(
        "s0",
        KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name())),
        ValueFormat.of(FormatInfo.of(
            FormatFactory.AVRO.name(), ImmutableMap.of(AvroFormat.FULL_SCHEMA_NAME, "org.ac.s1")))
    );

    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("FIELD1"), SqlTypes.BIGINT)
        .build();

    final KsqlStream<?> ksqlStream = new KsqlStream<>(
        "create stream s0 with(KAFKA_TOPIC='s0', VALUE_AVRO_SCHEMA_FULL_NAME='org.ac.s1', VALUE_FORMAT='avro');",
        SourceName.of("S0"),
        schema,
        SerdeOptions.of(),
        Optional.empty(),
        false,
        ksqlTopic
    );

    newAvroMetaStore.putSource(ksqlStream, false);

    final List<Statement> statements = parse(simpleQuery, newAvroMetaStore);
    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statements.get(0);
    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(newAvroMetaStore, "");
    final Analysis analysis = analyzer.analyze(query, Optional.of(createStreamAsSelect.getSink()));

    assertThat(analysis.getInto(), is(not(Optional.empty())));
    assertThat(analysis.getInto().get().getKsqlTopic().getValueFormat(),
        is(ValueFormat.of(FormatInfo.of(FormatFactory.AVRO.name()))));
  }

  @Test
  public void shouldUseImplicitNamespaceWhenFormatIsInheritedForAvro() {
    final String simpleQuery = "create stream s1 as select * from test1;";

    final List<Statement> statements = parse(simpleQuery, avroMetaStore);
    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statements.get(0);
    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(avroMetaStore, "");
    final Analysis analysis = analyzer.analyze(query, Optional.of(createStreamAsSelect.getSink()));

    assertThat(analysis.getInto(), is(not(Optional.empty())));
    assertThat(analysis.getInto().get().getKsqlTopic().getValueFormat(),
        is(ValueFormat.of(FormatInfo.of(FormatFactory.AVRO.name()))));
  }

  @Test
  public void shouldFailIfExplicitNamespaceIsProvidedForNonAvroTopic() {
    final String simpleQuery = "CREATE STREAM FOO WITH (VALUE_FORMAT='JSON', VALUE_AVRO_SCHEMA_FULL_NAME='com.custom.schema', KAFKA_TOPIC='TEST_TOPIC1') AS SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;";
    final List<Statement> statements = parse(simpleQuery, jsonMetaStore);
    final CreateStreamAsSelect createStreamAsSelect = (CreateStreamAsSelect) statements.get(0);
    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(jsonMetaStore, "");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> analyzer.analyze(query, Optional.of(createStreamAsSelect.getSink()))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "JSON does not support the following configs: [fullSchemaName]"));
  }

  @Test
  public void shouldFailIfExplicitNamespaceIsProvidedButEmpty() {
    final CreateStreamAsSelect createStreamAsSelect = parseSingle(
        "CREATE STREAM FOO "
            + "WITH (VALUE_FORMAT='AVRO', VALUE_AVRO_SCHEMA_FULL_NAME='', KAFKA_TOPIC='TEST_TOPIC1') "
            + "AS SELECT col0, col2, col3 FROM test1 WHERE col0 > 100;");

    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(jsonMetaStore, "");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> analyzer.analyze(query, Optional.of(createStreamAsSelect.getSink()))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "fullSchemaName cannot be empty. Format configuration: {fullSchemaName=}"
    ));
  }

  @Test
  public void shouldCaptureProjectionColumnRefs() {
    // Given:
    query = parseSingle("Select COL0, COL0 + COL1, SUBSTRING(COL2, 1) from TEST1;");

    // When:
    final Analysis analysis = analyzer.analyze(query, Optional.empty());

    // Then:
    assertThat(analysis.getSelectColumnNames(), containsInAnyOrder(
        COL0,
        COL1,
        COL2
    ));
  }

  @Test
  public void shouldThrowOnSelfJoin() {
    // Given:
    final CreateStreamAsSelect createStreamAsSelect = parseSingle(
        "CREATE STREAM FOO AS "
            + "SELECT * FROM test1 t1 JOIN test1 t2 ON t1.col0 = t2.col0;"
    );

    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(jsonMetaStore, "");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> analyzer.analyze(query, Optional.of(createStreamAsSelect.getSink()))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Can not join 'TEST1' to 'TEST1': self joins are not yet supported."));
  }

  @Test
  public void shouldFailOnJoinWithoutSource() {
    // Given:
    final CreateStreamAsSelect createStreamAsSelect = parseSingle(
        "CREATE STREAM FOO AS "
            + "SELECT * FROM test1 t1 JOIN test2 t2 ON t1.col0 = 'foo';"
    );

    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(jsonMetaStore, "");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> analyzer.analyze(query, Optional.of(createStreamAsSelect.getSink()))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Invalid comparison expression ''foo'' in join "
            + "'(T1.COL0 = 'foo')'. Each side of the join comparision must contain references "
            + "from exactly one source."));
  }

  @Test
  public void shouldFailOnJoinOnOverlappingSources() {
    // Given:
    final CreateStreamAsSelect createStreamAsSelect = parseSingle(
        "CREATE STREAM FOO AS "
            + "SELECT * FROM test1 t1 JOIN test2 t2 ON t1.col0 + t2.col0 = t1.col0;"
    );

    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(jsonMetaStore, "");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> analyzer.analyze(query, Optional.of(createStreamAsSelect.getSink()))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Invalid comparison expression '(T1.COL0 + T2.COL0)' in "
            + "join '((T1.COL0 + T2.COL0) = T1.COL0)'. Each side of the join comparision must "
            + "contain references from exactly one source."));
  }

  @Test
  public void shouldFailOnSelfJoinInCondition() {
    // Given:
    final CreateStreamAsSelect createStreamAsSelect = parseSingle(
        "CREATE STREAM FOO AS "
            + "SELECT * FROM test1 t1 JOIN test2 t2 ON t1.col0 = t1.col0;"
    );

    final Query query = createStreamAsSelect.getQuery();

    final Analyzer analyzer = new Analyzer(jsonMetaStore, "");

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> analyzer.analyze(query, Optional.of(createStreamAsSelect.getSink()))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Each side of the join must reference exactly one source "
            + "and not the same source. Left side references `T1` and right references `T1`"));
  }

  @SuppressWarnings("unchecked")
  private <T extends Statement> T parseSingle(final String simpleQuery) {
    return (T) Iterables.getOnlyElement(parse(simpleQuery, jsonMetaStore));
  }

  private void registerKafkaSource() {
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(COL0, SqlTypes.BIGINT)
        .build();

    final KsqlTopic topic = new KsqlTopic(
        "ks",
        KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name())),
        ValueFormat.of(FormatInfo.of(FormatFactory.KAFKA.name()))
    );

    final KsqlStream<?> stream = new KsqlStream<>(
        "sqlexpression",
        SourceName.of("KAFKA_SOURCE"),
        schema,
        SerdeOptions.of(),
        Optional.empty(),
        false,
        topic
    );

    jsonMetaStore.putSource(stream, false);
  }

  private static List<Statement> parse(final String simpleQuery, final MetaStore metaStore) {
    return KsqlParserTestUtil.buildAst(simpleQuery, metaStore)
        .stream()
        .map(PreparedStatement::getStatement)
        .collect(Collectors.toList());
  }
}
