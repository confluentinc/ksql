/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.topic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SourceTopicsExtractorTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("F1"), SqlTypes.STRING)
      .build();

  private static final KsqlTopic TOPIC_1 = new KsqlTopic(
      "topic1",
      KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name()), SerdeFeatures.of()),
      ValueFormat.of(FormatInfo.of(FormatFactory.JSON.name()), SerdeFeatures.of())
  );

  private static final KsqlTopic TOPIC_2 = new KsqlTopic(
      "topic2",
      KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name()), SerdeFeatures.of()),
      ValueFormat.of(FormatInfo.of(FormatFactory.JSON.name()), SerdeFeatures.of())
  );

  private static final String STREAM_TOPIC_1 = "s1";
  private static final String STREAM_TOPIC_2 = "s2";

  @Mock
  private ServiceContext serviceContext;

  private SourceTopicsExtractor extractor;
  private KsqlEngine ksqlEngine;
  private MutableMetaStore metaStore;

  @Before
  public void setUp() {
    metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    ksqlEngine = KsqlEngineTestUtil.createKsqlEngine(
        serviceContext,
        metaStore,
        new MetricCollectors()
    );
    extractor = new SourceTopicsExtractor(metaStore);

    givenStreamWithTopic(STREAM_TOPIC_1, TOPIC_1);
    givenStreamWithTopic(STREAM_TOPIC_2, TOPIC_2);
  }

  @After
  public void closeEngine() {
    ksqlEngine.close();
  }

  private Statement givenStatement(final String sql) {
    return ksqlEngine.prepare(ksqlEngine.parse(sql).get(0)).getStatement();
  }

  @Test
  public void shouldExtractTopicFromSimpleSelect() {
    // Given:
    final Statement statement = givenStatement("SELECT * FROM " + STREAM_TOPIC_1 + ";");

    // When:
    extractor.process(statement, null);

    // Then:
    assertThat(extractor.getPrimarySourceTopic(), is(TOPIC_1));
  }

  @Test
  public void shouldExtractPrimaryTopicFromJoinSelect() {
    // Given:
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", STREAM_TOPIC_1, STREAM_TOPIC_2
    ));

    // When:
    extractor.process(statement, null);

    // Then:
    assertThat(extractor.getPrimarySourceTopic(), is(TOPIC_1));
  }

  @Test
  public void shouldExtractJoinTopicsFromJoinSelect() {
    // Given:
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", STREAM_TOPIC_1, STREAM_TOPIC_2
    ));

    // When:
    extractor.process(statement, null);

    // Then:
    assertThat(extractor.getSourceTopics(), contains(TOPIC_2, TOPIC_1));
  }

  @Test
  public void shouldFailIfSourceTopicNotInMetastore() {
    // Given:
    final Statement statement = givenStatement("SELECT * FROM " + STREAM_TOPIC_1 + ";");
    metaStore.deleteSource(SourceName.of(STREAM_TOPIC_1.toUpperCase()));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> extractor.process(statement, null)
    );

    // Then:
    assertThat(e.getMessage(), containsString(STREAM_TOPIC_1.toUpperCase() + " does not exist."));
  }

  private void givenStreamWithTopic(
      final String streamName,
      final KsqlTopic sourceTopic
  ) {
    final KsqlStream<?> streamSource = new KsqlStream<>(
        "",
        SourceName.of(streamName.toUpperCase()),
        SCHEMA,
        Optional.empty(),
        false,
        sourceTopic,
        false
    );

    metaStore.putSource(streamSource, false);
  }
}
