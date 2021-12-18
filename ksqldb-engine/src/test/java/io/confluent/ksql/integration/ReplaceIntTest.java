/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.integration;

import static io.confluent.ksql.GenericKey.genericKey;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.test.util.TopicTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.TestDataProvider;
import java.util.Map;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({IntegrationTest.class})
public class ReplaceIntTest {

  @ClassRule
  public static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  @Rule
  public final TestKsqlContext ksqlContext = TEST_HARNESS.ksqlContextBuilder()
      .withAdditionalConfig(KsqlConfig.KSQL_CREATE_OR_REPLACE_ENABLED, true)
      .withAdditionalConfig(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED, false)
      .build();

  private String inputTopic;

  @Before
  public void before() {
    inputTopic = TopicTestUtil.uniqueTopicName("Orders");

    TEST_HARNESS.ensureTopics(inputTopic);

    ksqlContext.sql(
        String.format(
            "CREATE STREAM source (%s) WITH (kafka_topic='%s', value_format='JSON');",
            Provider.SQL,
            inputTopic
        )
    );
  }

  @Test
  public void shouldReplaceDDL()  {
    // When:
    ksqlContext.sql(
        String.format(
            "CREATE OR REPLACE STREAM source (%s) WITH (kafka_topic='%s', value_format='JSON');",
            Provider.SQL + ", col3 BIGINT",
            inputTopic
        )
    );

    // Then:
    DataSource source = ksqlContext.getMetaStore().getSource(SourceName.of("SOURCE"));
    assertThat(source.getSchema().value().size(), is(3));
    assertThat(source.getSchema().value().get(2), is(Column.of(ColumnName.of("COL3"), SqlTypes.BIGINT, Namespace.VALUE, 2)));
  }

  @Test
  public void shouldReplaceSimpleProject() {
    // Given:
    final String outputTopic = TopicTestUtil.uniqueTopicName("");
    ksqlContext.sql(
        String.format(
            "CREATE STREAM project WITH(kafka_topic='%s') AS SELECT k, col1 FROM source;",
            outputTopic));

    TEST_HARNESS.produceRows(inputTopic, new Provider("1", "A", 1), FormatFactory.KAFKA,
        FormatFactory.JSON);
    assertForSource("PROJECT", outputTopic,
        ImmutableMap.of(genericKey("1"), GenericRow.genericRow("A")));

    // When:
    ksqlContext.sql(
        String.format(
            "CREATE OR REPLACE STREAM project WITH(kafka_topic='%s') AS SELECT k, col1, col2 FROM source;",
            outputTopic));
    TEST_HARNESS.produceRows(inputTopic, new Provider("2", "B", 2), FormatFactory.KAFKA,
        FormatFactory.JSON);

    // Then:
    final Map<GenericKey, GenericRow> expected = ImmutableMap.of(
        genericKey("1"), GenericRow.genericRow("A", null),
        // this row is leftover from the original query
        genericKey("2"), GenericRow.genericRow("B", 2)
        // this row is an artifact from the new query
    );
    assertForSource("PROJECT", outputTopic, expected);
  }

  @Test
  public void shouldReplaceSimpleFilter() {
    // Given:
    final String outputTopic = TopicTestUtil.uniqueTopicName("");
    ksqlContext.sql(
        String.format(
            "CREATE STREAM project WITH(kafka_topic='%s') AS SELECT k, col1 FROM source;",
            outputTopic));

    TEST_HARNESS.produceRows(inputTopic, new Provider("1", "A", 1), FormatFactory.KAFKA,
        FormatFactory.JSON);
    assertForSource("PROJECT", outputTopic,
        ImmutableMap.of(genericKey("1"), GenericRow.genericRow("A")));

    // When:
    ksqlContext.sql(
        String.format(
            "CREATE OR REPLACE STREAM project WITH(kafka_topic='%s') AS SELECT k, col1 FROM source WHERE col1 <> 'A';",
            outputTopic));
    TEST_HARNESS.produceRows(inputTopic, new Provider("2", "A", 2), FormatFactory.KAFKA,
        FormatFactory.JSON); // this row should be filtered out
    TEST_HARNESS.produceRows(inputTopic, new Provider("3", "C", 3), FormatFactory.KAFKA,
        FormatFactory.JSON);

    // Then:
    final Map<GenericKey, GenericRow> expected = ImmutableMap.of(
        genericKey("1"), GenericRow.genericRow("A"),
        // this row is leftover from the original query
        genericKey("3"), GenericRow.genericRow("C")   // this row is an artifact from the new query
    );
    assertForSource("PROJECT", outputTopic, expected);
  }

  private void assertForSource(
      final String sourceName,
      final String topic,
      final Map<GenericKey, GenericRow> expected
  ) {
    DataSource source = ksqlContext.getMetaStore().getSource(SourceName.of(sourceName));
    PhysicalSchema resultSchema = PhysicalSchema.from(
        source.getSchema(),
        source.getKsqlTopic().getKeyFormat().getFeatures(),
        source.getKsqlTopic().getValueFormat().getFeatures()
    );

    assertThat(
        TEST_HARNESS.verifyAvailableUniqueRows(topic, expected.size(), FormatFactory.KAFKA, FormatFactory.JSON, resultSchema),
        is(expected)
    );
  }

  private static class Provider extends TestDataProvider {

    private static final String SQL = "k VARCHAR KEY, col1 VARCHAR, col2 INT";
    private static final LogicalSchema LOGICAL_SCHEMA = LogicalSchema.builder()
        .keyColumn(ColumnName.of("K"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("COL1"), SqlTypes.STRING)
        .valueColumn(ColumnName.of("COL2"), SqlTypes.INTEGER)
        .build();

    public Provider(final String k, final String col1, final int col2) {
      super(
          "SOURCE",
          PhysicalSchema.from(LOGICAL_SCHEMA, SerdeFeatures.of(), SerdeFeatures.of()),
          rows(k, col1, col2)
      );
    }

    private static Multimap<GenericKey, GenericRow> rows(
        final String key,
        final String col1,
        final Integer col2
    ) {
      return ImmutableListMultimap.of(genericKey(key), GenericRow.genericRow(col1, col2));
    }
  }

}
