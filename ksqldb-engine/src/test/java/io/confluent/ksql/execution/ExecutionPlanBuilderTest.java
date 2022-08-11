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

package io.confluent.ksql.execution;

import static io.confluent.ksql.metastore.model.MetaStoreMatchers.hasValueSerdeFeatures;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlConfigTestUtil;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.services.FakeKafkaTopicClient;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ExecutionPlanBuilderTest {

  private static final String CREATE_STREAM_TEST1 = "CREATE STREAM TEST1 "
      + "(ROWKEY STRING KEY, COL0 BIGINT, COL1 VARCHAR, COL2 DOUBLE) "
      + "WITH (KAFKA_TOPIC = 'test1', KEY_FORMAT = 'KAFKA', VALUE_FORMAT = 'JSON');";

  private static final String CREATE_STREAM_TEST2 = "CREATE STREAM TEST2 "
      + "(ID2 BIGINT KEY, COL0 VARCHAR, COL1 BIGINT) "
      + " WITH (KAFKA_TOPIC = 'test2', KEY_FORMAT = 'KAFKA', VALUE_FORMAT = 'JSON');";

  private static final String CREATE_STREAM_TEST3 = "CREATE STREAM TEST3 "
      + "(ID3 BIGINT KEY, COL0 BIGINT, COL1 DOUBLE) "
      + " WITH (KAFKA_TOPIC = 'test3', KEY_FORMAT = 'KAFKA', VALUE_FORMAT = 'JSON');";

  private static final String CREATE_TABLE_TEST4 = "CREATE TABLE TEST4 "
      + "(ID BIGINT PRIMARY KEY, COL0 BIGINT, COL1 DOUBLE) "
      + " WITH (KAFKA_TOPIC = 'test4', KEY_FORMAT = 'KAFKA', VALUE_FORMAT = 'JSON');";

  private static final String CREATE_TABLE_TEST5 = "CREATE TABLE TEST5 "
      + "(ID BIGINT PRIMARY KEY, COL0 BIGINT, COL1 DOUBLE) "
      + " WITH (KAFKA_TOPIC = 'test5', KEY_FORMAT = 'KAFKA', VALUE_FORMAT = 'JSON');";

  private static final String simpleSelectFilter = "SELECT rowkey, col0, col2 FROM test1 WHERE col0 > 100 EMIT CHANGES;";
  private static final KsqlConfig INITIAL_CONFIG = KsqlConfigTestUtil.create("what-eva");
  private final KafkaTopicClient kafkaTopicClient = new FakeKafkaTopicClient();
  private KsqlEngine ksqlEngine;

  private ServiceContext serviceContext;

  private KsqlConfig ksqlConfig;
  private MetaStoreImpl engineMetastore;

  @Before
  public void before() {
    ksqlConfig = INITIAL_CONFIG;
    serviceContext = TestServiceContext.create(kafkaTopicClient);
    engineMetastore = new MetaStoreImpl(new InternalFunctionRegistry());
    ksqlEngine = KsqlEngineTestUtil.createKsqlEngine(
        serviceContext,
        engineMetastore,
        new MetricCollectors()
    );
  }

  @After
  public void after() {
    ksqlEngine.close();
    serviceContext.close();
  }

  private QueryMetadata buildQuery(final String query) {
    givenKafkaTopicsExist("test1");
    return execute(CREATE_STREAM_TEST1 + query).get(0);
  }

  private QueryMetadata buildTransientQuery(final String query) {
    givenKafkaTopicsExist("test1");
    execute(CREATE_STREAM_TEST1);
    return executeQuery(query);
  }

  @Test
  public void shouldHaveKStreamDataSource() {
    final PersistentQueryMetadata metadata = (PersistentQueryMetadata) buildQuery(
        "CREATE STREAM FOO AS " + simpleSelectFilter);
    assertThat(metadata.getDataSourceType().get(), equalTo(DataSourceType.KSTREAM));
  }

  @Test
  public void shouldMakeBareQuery() {
    final QueryMetadata queryMetadata = buildTransientQuery(simpleSelectFilter);
    assertThat(queryMetadata, instanceOf(TransientQueryMetadata.class));
  }

  @Test
  public void shouldBuildTransientQueryWithCorrectSchema() {
    // When:
    final QueryMetadata queryMetadata = buildTransientQuery(simpleSelectFilter);

    // Then:
    assertThat(queryMetadata.getLogicalSchema(), is(LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("COL2"), SqlTypes.DOUBLE)
        .build()
    ));
  }

  @Test
  public void shouldBuildPersistentQueryWithCorrectSchema() {
    // When:
    final QueryMetadata queryMetadata = buildQuery(
        "CREATE STREAM FOO AS " + simpleSelectFilter);

    // Then:
    assertThat(queryMetadata.getLogicalSchema(), is(LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("COL0"), SqlTypes.BIGINT)
        .valueColumn(ColumnName.of("COL2"), SqlTypes.DOUBLE)
        .build()
    ));
  }

  @Test
  public void shouldMakePersistentQuery() {
    // Given:
    givenKafkaTopicsExist("test1");

    // When:
    final QueryMetadata queryMetadata =
        buildQuery("CREATE STREAM FOO AS " + simpleSelectFilter);

    // Then:
    assertThat(queryMetadata, instanceOf(PersistentQueryMetadata.class));
  }

  @Test
  public void shouldCreateExecutionPlanForInsert() {
    final String csasQuery = "CREATE STREAM s1 WITH (value_format = 'delimited') AS SELECT rowkey, col0, col1, "
        + "col2 FROM "
        + "test1;";
    final String insertIntoQuery = "INSERT INTO s1 SELECT rowkey, col0, col1, col2 FROM test1;";
    givenKafkaTopicsExist("test1");

    final List<QueryMetadata> queryMetadataList = execute(
        CREATE_STREAM_TEST1 + csasQuery + insertIntoQuery);
    assertThat(queryMetadataList, hasSize(2));
    final String planText = queryMetadataList.get(1).getExecutionPlan();
    final String[] lines = planText.split("\n");
    assertThat(lines.length, is(3));
    Assert.assertEquals(lines[0],
        " > [ SINK ] | Schema: ROWKEY STRING KEY, COL0 BIGINT, COL1 STRING, COL2 DOUBLE | Logger: INSERTQUERY_1.S1");
    Assert.assertEquals(lines[1],
        "\t\t > [ PROJECT ] | Schema: ROWKEY STRING KEY, COL0 BIGINT, COL1 STRING, COL2 DOUBLE | Logger: INSERTQUERY_1.Project");
    Assert.assertEquals(lines[2],
        "\t\t\t\t > [ SOURCE ] | Schema: ROWKEY STRING KEY, COL0 BIGINT, COL1 STRING, COL2 DOUBLE, ROWTIME BIGINT, ROWPARTITION INTEGER, ROWOFFSET BIGINT, ROWKEY STRING | Logger: INSERTQUERY_1.KsqlTopic.Source");
    assertThat(queryMetadataList.get(1), instanceOf(PersistentQueryMetadata.class));
    final PersistentQueryMetadata persistentQuery = (PersistentQueryMetadata)
        queryMetadataList.get(1);
    assertThat(persistentQuery.getResultTopic().get().getValueFormat().getFormat(),
        equalTo(FormatFactory.DELIMITED.name()));
  }

  @Test
  public void shouldCreatePlanForInsertIntoStreamFromStream() {
    // Given:
    final String cs = "CREATE STREAM test1 (ROWKEY STRING KEY, col0 INT) "
        + "WITH (KAFKA_TOPIC='test1', KEY_FORMAT = 'KAFKA', VALUE_FORMAT='JSON');";
    final String csas = "CREATE STREAM s0 AS SELECT * FROM test1;";
    final String insertInto = "INSERT INTO s0 SELECT * FROM test1;";
    givenKafkaTopicsExist("test1");

    // When:
    final List<QueryMetadata> queries = execute(cs + csas + insertInto);

    // Then:
    assertThat(queries, hasSize(2));
    final String planText = queries.get(1).getExecutionPlan();
    final String[] lines = planText.split("\n");
    assertThat(lines.length, equalTo(3));
    assertThat(lines[0], containsString(
        "> [ SINK ] | Schema: ROWKEY STRING KEY, COL0 INTEGER"));

    assertThat(lines[1], containsString(
        "> [ PROJECT ] | Schema: ROWKEY STRING KEY, COL0 INTEGER"));

    assertThat(lines[2], containsString(
        "> [ SOURCE ] | Schema: ROWKEY STRING KEY, COL0 INTEGER, ROWTIME BIGINT, ROWPARTITION INTEGER, ROWOFFSET BIGINT, ROWKEY STRING"));
  }

  @Test
  public void shouldRepartitionLeftStreamIfNotCorrectKey() {
    // Given:
    givenKafkaTopicsExist("test2", "test3");
    execute(CREATE_STREAM_TEST2 + CREATE_STREAM_TEST3);

    // When:
    final QueryMetadata result = execute("CREATE STREAM s1 AS "
        + "SELECT * FROM test2 JOIN test3 WITHIN 1 SECOND "
        + "ON test2.col1 = test3.id3;")
        .get(0);

    // Then:
    assertThat(result.getExecutionPlan(), containsString(
        "[ REKEY ] | Schema: COL1 BIGINT KEY,"
    ));
  }

  @Test
  public void shouldRepartitionRightStreamIfNotCorrectKey() {
    // Given:
    givenKafkaTopicsExist("test2", "test3");
    execute(CREATE_STREAM_TEST2 + CREATE_STREAM_TEST3);

    // When:
    final QueryMetadata result = execute("CREATE STREAM s1 AS "
        + "SELECT * FROM test2 JOIN test3 WITHIN 1 SECOND "
        + "ON test2.id2 = test3.col0;")
        .get(0);

    // Then:
    assertThat(result.getExecutionPlan(), containsString(
        "[ REKEY ] | Schema: COL0 BIGINT KEY,"
    ));
  }

  @Test
  public void shouldThrowIfRightTableNotJoiningOnTableKey() {
    // Given:
    givenKafkaTopicsExist("test4", "test5");
    execute(CREATE_TABLE_TEST4 + CREATE_TABLE_TEST5);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> execute("CREATE TABLE t1 AS "
            + "SELECT * FROM test4 JOIN test5 "
            + "ON test4.id = test5.col0;")
    );

    // Then:
    assertThat(
        e.getMessage(),
        containsString(
            "Invalid join condition:"
                + " table-table joins require to join on the primary key of the right input table."
                + " Got TEST4.ID = TEST5.COL0."
        )
    );
  }

  @Test
  public void shouldNotRepartitionEitherStreamsIfJoiningOnKeys() {
    // Given:
    givenKafkaTopicsExist("test2", "test3");
    execute(CREATE_STREAM_TEST2 + CREATE_STREAM_TEST3);

    // When:
    final QueryMetadata result = execute("CREATE STREAM s1 AS "
        + "SELECT * FROM test2 JOIN test3 WITHIN 1 SECOND "
        + "ON test2.id2 = test3.id3;")
        .get(0);

    // Then:
    assertThat(result.getExecutionPlan(), not(containsString("[ REKEY ]")));
  }

  @Test
  public void shouldGetSingleValueSchemaWrappingFromPropertiesBeforeConfig() {
    // Given:
    givenConfigWith(KsqlConfig.KSQL_WRAP_SINGLE_VALUES, true);
    givenKafkaTopicsExist("test1");
    execute(CREATE_STREAM_TEST1);

    // When:
    execute("CREATE STREAM TEST2 WITH(WRAP_SINGLE_VALUE=false) AS SELECT rowkey, COL0 FROM TEST1;");

    // Then:
    assertThat(engineMetastore.getSource(SourceName.of("TEST2")),
        hasValueSerdeFeatures(hasItem(SerdeFeature.UNWRAP_SINGLES)));
  }

  @Test
  public void shouldGetSingleValueSchemaWrappingFromConfig() {
    // Given:
    givenConfigWith(KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false);
    givenKafkaTopicsExist("test4");
    execute(CREATE_TABLE_TEST4);

    // When:
    execute("CREATE TABLE TEST5 AS SELECT ID, COL0 FROM TEST4;");

    // Then:
    assertThat(engineMetastore.getSource(SourceName.of("TEST5")),
        hasValueSerdeFeatures(hasItem(SerdeFeature.UNWRAP_SINGLES)));
  }

  @Test
  public void shouldDefaultToWrappingSingleValueSchemas() {
    // Given:
    givenKafkaTopicsExist("test4");
    execute(CREATE_TABLE_TEST4);

    // When:
    execute("CREATE TABLE TEST5 AS SELECT ID, COL0 FROM TEST4;");

    // Then:
    assertThat(engineMetastore.getSource(SourceName.of("TEST5")),
        hasValueSerdeFeatures(not(hasItem(SerdeFeature.UNWRAP_SINGLES))));
  }

  @SuppressWarnings("SameParameterValue")
  private void givenConfigWith(final String name, final Object value) {
    ksqlConfig = ksqlConfig.cloneWithPropertyOverwrite(ImmutableMap.of(name, value));
  }

  private List<QueryMetadata> execute(final String sql) {
    return KsqlEngineTestUtil.execute(
        serviceContext,
        ksqlEngine,
        sql,
        ksqlConfig,
        Collections.emptyMap());
  }

  private TransientQueryMetadata executeQuery(final String sql) {
    return KsqlEngineTestUtil.executeQuery(
        serviceContext, ksqlEngine, sql, ksqlConfig, Collections.emptyMap());
  }

  private void givenKafkaTopicsExist(final String... names) {
    final Map<String, ?> config = ImmutableMap.of(TopicConfig.RETENTION_MS_CONFIG, 5000L);
    Arrays.stream(names).forEach(name ->
        kafkaTopicClient.createTopic(name, 1, (short) 1, config)
    );
  }
}
