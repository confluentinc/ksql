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

package io.confluent.ksql.physical;

import static io.confluent.ksql.metastore.model.MetaStoreMatchers.hasSerdeOptions;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlConfigTestUtil;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.SerdeOption;
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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PhysicalPlanBuilderTest {

  private static final String CREATE_STREAM_TEST1 = "CREATE STREAM TEST1 "
      + "(COL0 BIGINT, COL1 VARCHAR, COL2 DOUBLE) "
      + "WITH (KAFKA_TOPIC = 'test1', VALUE_FORMAT = 'JSON');";

  private static final String CREATE_STREAM_TEST2 = "CREATE STREAM TEST2 "
      + "(ID BIGINT, COL0 VARCHAR, COL1 DOUBLE) "
      + " WITH (KAFKA_TOPIC = 'test2', VALUE_FORMAT = 'JSON', KEY='ID');";

  private static final String CREATE_STREAM_TEST3 = "CREATE STREAM TEST3 "
      + "(ID BIGINT, COL0 VARCHAR, COL1 DOUBLE) "
      + " WITH (KAFKA_TOPIC = 'test3', VALUE_FORMAT = 'JSON', KEY='ID');";

  private static final String CREATE_TABLE_TEST4 = "CREATE TABLE TEST4 "
      + "(ID BIGINT, COL0 VARCHAR, COL1 DOUBLE) "
      + " WITH (KAFKA_TOPIC = 'test4', VALUE_FORMAT = 'JSON', KEY='ID');";

  private static final String CREATE_TABLE_TEST5 = "CREATE TABLE TEST5 "
      + "(ID BIGINT, COL0 VARCHAR, COL1 DOUBLE) "
      + " WITH (KAFKA_TOPIC = 'test5', VALUE_FORMAT = 'JSON', KEY='ID');";

  private static final String CREATE_STREAM_TEST6 = "CREATE STREAM TEST6 "
      + "(ID BIGINT, COL0 VARCHAR, COL1 DOUBLE) "
      + " WITH (KAFKA_TOPIC = 'test6', VALUE_FORMAT = 'JSON');";

  private static final String CREATE_STREAM_TEST7 = "CREATE STREAM TEST7 "
      + "(ID BIGINT, COL0 VARCHAR, COL1 DOUBLE) "
      + " WITH (KAFKA_TOPIC = 'test7', VALUE_FORMAT = 'JSON');";

  private static final String simpleSelectFilter = "SELECT col0, col2 FROM test1 WHERE col0 > 100 EMIT CHANGES;";
  private static final KsqlConfig INITIAL_CONFIG = KsqlConfigTestUtil.create("what-eva");
  private final KafkaTopicClient kafkaTopicClient = new FakeKafkaTopicClient();
  private KsqlEngine ksqlEngine;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

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
        engineMetastore
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

  @Test
  public void shouldHaveKStreamDataSource() {
    final PersistentQueryMetadata metadata = (PersistentQueryMetadata) buildQuery(
        "CREATE STREAM FOO AS " + simpleSelectFilter);
    assertThat(metadata.getDataSourceType(), equalTo(DataSourceType.KSTREAM));
  }

  @Test
  public void shouldMakeBareQuery() {
    final QueryMetadata queryMetadata = buildQuery(simpleSelectFilter);
    assertThat(queryMetadata, instanceOf(TransientQueryMetadata.class));
  }

  @Test
  public void shouldBuildTransientQueryWithCorrectSchema() {
    // When:
    final QueryMetadata queryMetadata = buildQuery(simpleSelectFilter);

    // Then:
    assertThat(queryMetadata.getLogicalSchema(), is(LogicalSchema.builder()
        .noImplicitColumns()
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
  public void shouldCreateExecutionPlan() {
    final String queryString =
        "CREATE STREAM TEST1 ("
            + "COL0 BIGINT, COL1 VARCHAR, COL2 STRING, COL3 DOUBLE,"
            + " COL4 ARRAY<DOUBLE>, COL5 MAP<STRING, DOUBLE>)"
            + " WITH (KAFKA_TOPIC = 'test1', VALUE_FORMAT = 'JSON');"
            + "SELECT col0, sum(col3), count(col3) FROM test1"
            + " WHERE col0 > 100 GROUP BY col0 EMIT CHANGES;";
    givenKafkaTopicsExist("test1");
    final QueryMetadata metadata = execute(queryString).get(0);
    final String planText = metadata.getExecutionPlan();
    final String[] lines = planText.split("\n");
    assertThat(lines[0], startsWith(
        " > [ PROJECT ] | Schema: [ROWKEY STRING KEY, COL0 BIGINT, KSQL_COL_1 DOUBLE, "
            + "KSQL_COL_2 BIGINT] |"));
    assertThat(lines[1], startsWith(
        "\t\t > [ AGGREGATE ] | Schema: [ROWKEY STRING KEY, KSQL_INTERNAL_COL_0 BIGINT, "
            + "KSQL_INTERNAL_COL_1 DOUBLE, KSQL_AGG_VARIABLE_0 DOUBLE, "
            + "KSQL_AGG_VARIABLE_1 BIGINT] |"));
    assertThat(lines[2], startsWith(
        "\t\t\t\t > [ PROJECT ] | Schema: [ROWKEY STRING KEY, KSQL_INTERNAL_COL_0 BIGINT, "
            + "KSQL_INTERNAL_COL_1 DOUBLE] |"));
    assertThat(lines[3], startsWith(
        "\t\t\t\t\t\t > [ FILTER ] | Schema: [TEST1.ROWKEY STRING KEY, TEST1.ROWTIME BIGINT, TEST1.ROWKEY STRING, "
            + "TEST1.COL0 BIGINT, TEST1.COL1 STRING, TEST1.COL2 STRING, "
            + "TEST1.COL3 DOUBLE, TEST1.COL4 ARRAY<DOUBLE>, "
            + "TEST1.COL5 MAP<STRING, DOUBLE>] |"));
    assertThat(lines[4], startsWith(
        "\t\t\t\t\t\t\t\t > [ SOURCE ] | Schema: [TEST1.ROWKEY STRING KEY, TEST1.ROWTIME BIGINT, TEST1.ROWKEY STRING, "
            + "TEST1.COL0 BIGINT, TEST1.COL1 STRING, TEST1.COL2 STRING, "
            + "TEST1.COL3 DOUBLE, TEST1.COL4 ARRAY<DOUBLE>, "
            + "TEST1.COL5 MAP<STRING, DOUBLE>] |"));
  }

  @Test
  public void shouldCreateExecutionPlanForInsert() {
    final String csasQuery = "CREATE STREAM s1 WITH (value_format = 'delimited') AS SELECT col0, col1, "
        + "col2 FROM "
        + "test1;";
    final String insertIntoQuery = "INSERT INTO s1 SELECT col0, col1, col2 FROM test1;";
    givenKafkaTopicsExist("test1");

    final List<QueryMetadata> queryMetadataList = execute(
        CREATE_STREAM_TEST1 + csasQuery + insertIntoQuery);
    Assert.assertTrue(queryMetadataList.size() == 2);
    final String planText = queryMetadataList.get(1).getExecutionPlan();
    final String[] lines = planText.split("\n");
    Assert.assertTrue(lines.length == 3);
    Assert.assertEquals(lines[0],
        " > [ SINK ] | Schema: [ROWKEY STRING KEY, COL0 BIGINT, COL1 STRING, COL2 DOUBLE] | Logger: InsertQuery_1.S1");
    Assert.assertEquals(lines[1],
        "\t\t > [ PROJECT ] | Schema: [ROWKEY STRING KEY, COL0 BIGINT, COL1 STRING, COL2 DOUBLE] | Logger: InsertQuery_1.Project");
    Assert.assertEquals(lines[2],
        "\t\t\t\t > [ SOURCE ] | Schema: [TEST1.ROWKEY STRING KEY, TEST1.ROWTIME BIGINT, TEST1.ROWKEY STRING, TEST1.COL0 BIGINT, TEST1.COL1 STRING, TEST1.COL2 DOUBLE] | Logger: InsertQuery_1.KsqlTopic.source");
    assertThat(queryMetadataList.get(1), instanceOf(PersistentQueryMetadata.class));
    final PersistentQueryMetadata persistentQuery = (PersistentQueryMetadata)
        queryMetadataList.get(1);
    assertThat(persistentQuery.getResultTopic().getValueFormat().getFormat(),
        equalTo(Format.DELIMITED));
  }

  @Test
  public void shouldCreatePlanForInsertIntoStreamFromStream() {
    // Given:
    final String cs = "CREATE STREAM test1 (col0 INT) "
        + "WITH (KAFKA_TOPIC='test1', VALUE_FORMAT='JSON');";
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
        "> [ SINK ] | Schema: [ROWKEY STRING KEY, COL0 INTEGER]"));

    assertThat(lines[1], containsString(
        "> [ PROJECT ] | Schema: [ROWKEY STRING KEY, ROWTIME BIGINT, ROWKEY STRING, COL0 INTEGER]"));

    assertThat(lines[2], containsString(
        "> [ SOURCE ] | Schema: [TEST1.ROWKEY STRING KEY, TEST1.ROWTIME BIGINT, TEST1.ROWKEY STRING, TEST1.COL0 INTEGER]"));
  }

  @Test
  public void shouldRekeyIfPartitionByDoesNotMatchResultKey() {
    final String csasQuery = "CREATE STREAM s1 AS SELECT col0, col1, col2 FROM test1 PARTITION BY col0;";
    final String insertIntoQuery = "INSERT INTO s1 SELECT col0, col1, col2 FROM test1 PARTITION BY col0;";
    givenKafkaTopicsExist("test1");

    final List<QueryMetadata> queryMetadataList = execute(
        CREATE_STREAM_TEST1 + csasQuery + insertIntoQuery);
    assertThat(queryMetadataList, hasSize(2));
    final String planText = queryMetadataList.get(1).getExecutionPlan();
    final String[] lines = planText.split("\n");
    assertThat(lines.length, equalTo(4));
    assertThat(lines[0], equalTo(" > [ SINK ] | Schema: [ROWKEY STRING KEY, COL0 BIGINT, COL1 STRING, COL2 "
        + "DOUBLE] | Logger: InsertQuery_1.S1"));
    assertThat(lines[1],
        equalTo("\t\t > [ REKEY ] | Schema: [ROWKEY STRING KEY, COL0 BIGINT, COL1 STRING, COL2 DOUBLE] "
            + "| Logger: InsertQuery_1.S1"));
    assertThat(lines[2], equalTo("\t\t\t\t > [ PROJECT ] | Schema: [ROWKEY STRING KEY, COL0 BIGINT, COL1 STRING"
        + ", COL2 DOUBLE] | Logger: InsertQuery_1.Project"));
  }

  @Test
  public void shouldRepartitionLeftStreamIfNotCorrectKey() {
    // Given:
    givenKafkaTopicsExist("test2", "test3");
    execute(CREATE_STREAM_TEST2 + CREATE_STREAM_TEST3);

    // When:
    final QueryMetadata result = execute("CREATE STREAM s1 AS "
        + "SELECT * FROM test2 JOIN test3 WITHIN 1 SECOND "
        + "ON test2.col1 = test3.id;")
        .get(0);

    // Then:
    assertThat(result.getExecutionPlan(), containsString("[ REKEY ] | Schema: [TEST2."));
  }

  @Test
  public void shouldRepartitionRightStreamIfNotCorrectKey() {
    // Given:
    givenKafkaTopicsExist("test2", "test3");
    execute(CREATE_STREAM_TEST2 + CREATE_STREAM_TEST3);

    // When:
    final QueryMetadata result = execute("CREATE STREAM s1 AS "
        + "SELECT * FROM test2 JOIN test3 WITHIN 1 SECOND "
        + "ON test2.id = test3.col0;")
        .get(0);

    // Then:
    assertThat(result.getExecutionPlan(), containsString("[ REKEY ] | Schema: [TEST3."));
  }

  @Test
  public void shouldThrowIfLeftTableNotJoiningOnTableKey() {
    // Given:
    givenKafkaTopicsExist("test4", "test5");
    execute(CREATE_TABLE_TEST4 + CREATE_TABLE_TEST5);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Source table (TEST4) key column (TEST4.ID) is not the column "
            + "used in the join criteria (TEST4.COL0).");

    // When:
    execute("CREATE TABLE t1 AS "
        + "SELECT * FROM test4 JOIN test5 "
        + "ON test4.col0 = test5.id;");
  }

  @Test
  public void shouldThrowIfRightTableNotJoiningOnTableKey() {
    // Given:
    givenKafkaTopicsExist("test4", "test5");
    execute(CREATE_TABLE_TEST4 + CREATE_TABLE_TEST5);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(
        "Source table (TEST5) key column (TEST5.ID) is not the column "
            + "used in the join criteria (TEST5.COL0).");

    // When:
    execute("CREATE TABLE t1 AS "
        + "SELECT * FROM test4 JOIN test5 "
        + "ON test4.id = test5.col0;");
  }

  @Test
  public void shouldNotRepartitionEitherStreamsIfJoiningOnKeys() {
    // Given:
    givenKafkaTopicsExist("test2", "test3");
    execute(CREATE_STREAM_TEST2 + CREATE_STREAM_TEST3);

    // When:
    final QueryMetadata result = execute("CREATE STREAM s1 AS "
        + "SELECT * FROM test2 JOIN test3 WITHIN 1 SECOND "
        + "ON test2.id = test3.id;")
        .get(0);

    // Then:
    assertThat(result.getExecutionPlan(), not(containsString("[ REKEY ]")));
  }

  @Test
  public void shouldNotRepartitionEitherStreamsIfJoiningOnRowKey() {
    // Given:
    givenKafkaTopicsExist("test2", "test3");
    execute(CREATE_STREAM_TEST2 + CREATE_STREAM_TEST3);

    // When:
    final QueryMetadata result = execute("CREATE STREAM s1 AS "
        + "SELECT * FROM test2 JOIN test3 WITHIN 1 SECOND "
        + "ON test2.rowkey = test3.rowkey;")
        .get(0);

    // Then:
    assertThat(result.getExecutionPlan(), not(containsString("[ REKEY ]")));
  }

  @Test
  public void shouldNotRepartitionEitherStreamsIfJoiningOnRowKeyEvenIfStreamsHaveNoKeyField() {
    // Given:
    givenKafkaTopicsExist("test6", "test7");
    execute(CREATE_STREAM_TEST6 + CREATE_STREAM_TEST7);

    // When:
    final QueryMetadata result = execute("CREATE STREAM s1 AS "
        + "SELECT * FROM test6 JOIN test7 WITHIN 1 SECOND "
        + "ON test6.rowkey = test7.rowkey;")
        .get(0);

    // Then:
    assertThat(result.getExecutionPlan(), not(containsString("[ REKEY ]")));
  }

  @Test
  public void shouldHandleLeftTableJoiningOnRowKey() {
    // Given:
    givenKafkaTopicsExist("test4", "test5");
    execute(CREATE_TABLE_TEST4 + CREATE_TABLE_TEST5);

    // When:
    execute("CREATE TABLE t1 AS "
        + "SELECT * FROM test4 JOIN test5 "
        + "ON test4.rowkey = test5.id;");

    // Then: did not throw.
  }

  @Test
  public void shouldHandleRightTableJoiningOnRowKey() {
    // Given:
    givenKafkaTopicsExist("test4", "test5");
    execute(CREATE_TABLE_TEST4 + CREATE_TABLE_TEST5);

    // When:
    execute("CREATE TABLE t1 AS "
        + "SELECT * FROM test4 JOIN test5 "
        + "ON test4.id = test5.rowkey;");

    // Then: did not throw.
  }

  @Test
  public void shouldGetSingleValueSchemaWrappingFromPropertiesBeforeConfig() {
    // Given:
    givenConfigWith(KsqlConfig.KSQL_WRAP_SINGLE_VALUES, true);
    givenKafkaTopicsExist("test1");
    execute(CREATE_STREAM_TEST1);

    // When:
    execute("CREATE STREAM TEST2 WITH(WRAP_SINGLE_VALUE=false) AS SELECT COL0 FROM TEST1;");

    // Then:
    assertThat(engineMetastore.getSource(SourceName.of("TEST2")),
        hasSerdeOptions(hasItem(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldGetSingleValueSchemaWrappingFromConfig() {
    // Given:
    givenConfigWith(KsqlConfig.KSQL_WRAP_SINGLE_VALUES, false);
    givenKafkaTopicsExist("test4");
    execute(CREATE_TABLE_TEST4);

    // When:
    execute("CREATE TABLE TEST5 AS SELECT COL0 FROM TEST4;");

    // Then:
    assertThat(engineMetastore.getSource(SourceName.of("TEST5")),
        hasSerdeOptions(hasItem(SerdeOption.UNWRAP_SINGLE_VALUES)));
  }

  @Test
  public void shouldDefaultToWrappingSingleValueSchemas() {
    // Given:
    givenKafkaTopicsExist("test4");
    execute(CREATE_TABLE_TEST4);

    // When:
    execute("CREATE TABLE TEST5 AS SELECT COL0 FROM TEST4;");

    // Then:
    assertThat(engineMetastore.getSource(SourceName.of("TEST5")),
        hasSerdeOptions(not(hasItem(SerdeOption.UNWRAP_SINGLE_VALUES))));
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

  private void givenKafkaTopicsExist(final String... names) {
    Arrays.stream(names).forEach(name ->
        kafkaTopicClient.createTopic(name, 1, (short) 1, Collections.emptyMap())
    );
  }
}
