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

package io.confluent.ksql.rest.entity;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.isIn;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metrics.ConsumerCollector;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.metrics.StreamsErrorCollector;
import io.confluent.ksql.metrics.TopicSensors;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.util.KsqlHostInfo;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class SourceDescriptionFactoryTest {
  private final List<TopicSensors.Stat> errorStats = IntStream.range(0, 5)
      .boxed()
      .map((x) -> new TopicSensors.Stat(StreamsErrorCollector.CONSUMER_FAILED_MESSAGES, x, x))
      .collect(Collectors.toList());

  private final List<TopicSensors.Stat> stats = IntStream.range(0, 5)
      .boxed()
      .map((x) -> new TopicSensors.Stat(ConsumerCollector.CONSUMER_TOTAL_MESSAGES, x, x))
      .collect(Collectors.toList());

  private static final String mockStringStat = "mock_string_stat";

  private static DataSource buildDataSource(
      final String kafkaTopicName,
      final Optional<TimestampColumn> timestampColumn) {
    final LogicalSchema schema = LogicalSchema.builder()
        .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
        .valueColumn(ColumnName.of("field0"), SqlTypes.INTEGER)
        .build();

    final KsqlTopic topic = new KsqlTopic(
        kafkaTopicName,
        KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name()), SerdeFeatures.of()),
        ValueFormat.of(FormatInfo.of(FormatFactory.JSON.name()), SerdeFeatures.of())
    );

    return new KsqlStream<>(
        "query",
        SourceName.of("stream"),
        schema,
        timestampColumn,
        false,
        topic,
        false
    );
  }

  @Test
  public void shouldReturnLocalStatsBasedOnKafkaTopic() {
    // Given:
    final String kafkaTopicName = "kafka";
    final DataSource dataSource = buildDataSource(kafkaTopicName, Optional.empty());
    final MetricCollectors mock = Mockito.mock(MetricCollectors.class);

    Mockito.when(mock.getAndFormatStatsFor(anyString(), anyBoolean()))
        .thenReturn(mockStringStat);
    Mockito.when(mock.getStatsFor(dataSource.getKafkaTopicName(), true))
        .thenReturn(errorStats);
    Mockito.when(mock.getStatsFor(dataSource.getKafkaTopicName(), false))
        .thenReturn(stats);
    KsqlHostInfo localhost = new KsqlHostInfo("myhost", 10);
    // When
    final SourceDescription sourceDescription = SourceDescriptionFactory.create(
        dataSource,
        true,
        Collections.emptyList(),
        Collections.emptyList(),
        Optional.empty(),
        Collections.emptyList(),
        Collections.emptyList(),
        Stream.empty(),
        Stream.empty(),
        localhost,
        mock
    );

    // Then:
    // TODO deprecate and remove
    assertThat(
        sourceDescription.getStatistics(),
        containsString(mockStringStat));
    assertThat(
        sourceDescription.getErrorStats(),
        containsString(mockStringStat));
    // Also check includes its own stats in cluster stats
    final Stream<QueryHostStat> localStats = stats.stream()
        .map((s) -> QueryHostStat.fromStat(s, new KsqlHostInfoEntity(localhost)));
    assertThat(
        localStats.collect(Collectors.toList()),
        everyItem(isIn(sourceDescription.getClusterStatistics()))
    );

    final Stream<QueryHostStat> localErrors = errorStats.stream()
        .map((s) -> QueryHostStat.fromStat(s, new KsqlHostInfoEntity(localhost)));
    assertThat(
        localErrors.collect(Collectors.toList()),
        everyItem(isIn(sourceDescription.getClusterErrorStats()))
    );
  }

  @Test
  public void testShouldIncludeRemoteStatsIfProvided() {
    final List<QueryHostStat> remoteStats = IntStream.range(0, 5)
        .boxed()
        .map(x -> new QueryHostStat(
            new KsqlHostInfoEntity("otherhost:1090"),
            ConsumerCollector.CONSUMER_MESSAGES_PER_SEC,
            x,
            x)
        ).collect(Collectors.toList());
    final List<QueryHostStat> remoteErrors = IntStream.range(0, 5)
        .boxed()
        .map(x -> new QueryHostStat(
            new KsqlHostInfoEntity("otherhost:1090"),
            StreamsErrorCollector.CONSUMER_FAILED_MESSAGES_PER_SEC,
            x,
            x)
        ).collect(Collectors.toList());

    // Given:
    final String kafkaTopicName = "kafka";
    final DataSource dataSource = buildDataSource(kafkaTopicName, Optional.empty());
    final MetricCollectors mock = Mockito.mock(MetricCollectors.class);

    Mockito.when(mock.getAndFormatStatsFor(anyString(), anyBoolean())).thenReturn(mockStringStat);
    Mockito.when(mock.getStatsFor(dataSource.getKafkaTopicName(), true)).thenReturn(errorStats);
    Mockito.when(mock.getStatsFor(dataSource.getKafkaTopicName(), false)).thenReturn(stats);

    // When
    final SourceDescription sourceDescription = SourceDescriptionFactory.create(
        dataSource,
        true,
        Collections.emptyList(),
        Collections.emptyList(),
        Optional.empty(),
        Collections.emptyList(),
        Collections.emptyList(),
        remoteStats.stream(),
        remoteErrors.stream(),
        new KsqlHostInfo("myhost", 10),
        mock
    );

    // Then:
    assertThat(
        remoteStats,
        everyItem(isIn(sourceDescription.getClusterStatistics()))
    );
    assertThat(
        remoteErrors,
        everyItem(isIn(sourceDescription.getClusterErrorStats()))
    );
  }

  @Test
  public void shouldReturnEmptyTimestampColumn() {
    // Given:
    final String kafkaTopicName = "kafka";
    final DataSource dataSource = buildDataSource(kafkaTopicName, Optional.empty());

    // When
    final SourceDescription sourceDescription = SourceDescriptionFactory.create(
        dataSource,
        true,
        Collections.emptyList(),
        Collections.emptyList(),
        Optional.empty(),
        Collections.emptyList(),
        Collections.emptyList(),
        new MetricCollectors()
    );

    // Then:
    assertThat(sourceDescription.getTimestamp(), is(""));
  }

  @Test
  public void shouldReturnSourceConstraints() {
    // Given:
    final String kafkaTopicName = "kafka";
    final DataSource dataSource = buildDataSource(kafkaTopicName, Optional.empty());

    // When
    final SourceDescription sourceDescription = SourceDescriptionFactory.create(
        dataSource,
        true,
        Collections.emptyList(),
        Collections.emptyList(),
        Optional.empty(),
        Collections.emptyList(),
        ImmutableList.of("s1", "s2"),
        new MetricCollectors()
    );

    // Then:
    assertThat(sourceDescription.getSourceConstraints(), hasItems("s1", "s2"));
  }

  @Test
  public void shouldReturnTimestampColumnIfPresent() {
    // Given:
    final String kafkaTopicName = "kafka";
    final DataSource dataSource = buildDataSource(
        kafkaTopicName,
        Optional.of(
            new TimestampColumn(ColumnName.of("foo"), Optional.empty()))
    );

    // When
    final SourceDescription sourceDescription = SourceDescriptionFactory.create(
        dataSource,
        true,
        Collections.emptyList(),
        Collections.emptyList(),
        Optional.empty(),
        Collections.emptyList(),
        Collections.emptyList(),
        new MetricCollectors()
    );

    // Then:
    assertThat(sourceDescription.getTimestamp(), is("foo"));
  }
}
