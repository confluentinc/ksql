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

package io.confluent.ksql.rest.server.execution;
import static io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlWarning;
import io.confluent.ksql.rest.entity.QueryStatusCount;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.entity.SourceDescriptionEntity;
import io.confluent.ksql.rest.entity.SourceDescriptionFactory;
import io.confluent.ksql.rest.entity.SourceDescriptionList;
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlHostInfo;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.jeasy.random.EasyRandom;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ListSourceExecutorTest {

  EasyRandom objectMother = new EasyRandom();

  @Rule
  public final TemporaryEngine engine = new TemporaryEngine();

  @Mock
  private SessionProperties SESSION_PROPERTIES;
  @Mock
  private TopicDescription topicWith1PartitionAndRfOf1;

  private KsqlHostInfo ksqlHostInfo = objectMother.nextObject(KsqlHostInfo.class);

  @Before
  public void setUp() {
    final Node node = mock(Node.class);
    final TopicPartitionInfo topicInfo = mock(TopicPartitionInfo.class);
    when(topicInfo.replicas()).thenReturn(ImmutableList.of(node));
    when(topicWith1PartitionAndRfOf1.partitions()).thenReturn(ImmutableList.of(topicInfo));
    when(SESSION_PROPERTIES.getKsqlHostInfo()).thenReturn(ksqlHostInfo);
  }

  @Test
  public void shouldShowStreams() {
    // Given:
    final KsqlStream<?> stream1 = engine.givenSource(DataSourceType.KSTREAM, "stream1");
    final KsqlStream<?> stream2 = engine.givenSource(DataSourceType.KSTREAM, "stream2");
    engine.givenSource(DataSourceType.KTABLE, "table");

    // When:
    final StreamsList descriptionList = (StreamsList)
        CustomExecutors.LIST_STREAMS.execute(
            engine.configure("SHOW STREAMS;"),
            SESSION_PROPERTIES,
            engine.getEngine(),
            engine.getServiceContext()
        ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(descriptionList.getStreams(), containsInAnyOrder(
        new SourceInfo.Stream(
            stream1.getName().toString(FormatOptions.noEscape()),
            stream1.getKafkaTopicName(),
            stream1.getKsqlTopic().getKeyFormat().getFormat(),
            stream1.getKsqlTopic().getValueFormat().getFormat(),
            stream1.getKsqlTopic().getKeyFormat().isWindowed()
        ),
        new SourceInfo.Stream(
            stream2.getName().toString(FormatOptions.noEscape()),
            stream2.getKafkaTopicName(),
            stream2.getKsqlTopic().getKeyFormat().getFormat(),
            stream2.getKsqlTopic().getValueFormat().getFormat(),
            stream1.getKsqlTopic().getKeyFormat().isWindowed()
        )
    ));
  }

  @Test
  public void shouldShowStreamsExtended() {
    // Given:
    final KsqlStream<?> stream1 = engine.givenSource(DataSourceType.KSTREAM, "stream1");
    final KsqlStream<?> stream2 = engine.givenSource(DataSourceType.KSTREAM, "stream2",
        ImmutableSet.of(SourceName.of("stream1")));
    engine.givenSource(DataSourceType.KTABLE, "table");

    // When:
    final SourceDescriptionList descriptionList = (SourceDescriptionList)
        CustomExecutors.LIST_STREAMS.execute(
            engine.configure("SHOW STREAMS EXTENDED;"),
            SESSION_PROPERTIES,
            engine.getEngine(),
            engine.getServiceContext()
        ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(descriptionList.getSourceDescriptions(), containsInAnyOrder(
        SourceDescriptionFactory.create(
            stream1,
            true,
            ImmutableList.of(),
            ImmutableList.of(),
            Optional.of(topicWith1PartitionAndRfOf1),
            ImmutableList.of(),
            ImmutableList.of("stream2"),
            new MetricCollectors()
        ),
        SourceDescriptionFactory.create(
            stream2,
            true,
            ImmutableList.of(),
            ImmutableList.of(),
            Optional.of(topicWith1PartitionAndRfOf1),
            ImmutableList.of(),
            ImmutableList.of(),
            new MetricCollectors()
        )
    ));
  }

  @Test
  public void shouldDescribeStreams() {
    // Given:
    final KsqlStream<?> stream1 = engine.givenSource(DataSourceType.KSTREAM, "stream1");
    final KsqlStream<?> stream2 = engine.givenSource(DataSourceType.KSTREAM, "stream2",
        ImmutableSet.of(SourceName.of("stream1")));
    engine.givenSource(DataSourceType.KTABLE, "table");

    // When:
    final SourceDescriptionList descriptionList = (SourceDescriptionList)
        CustomExecutors.DESCRIBE_STREAMS.execute(
            engine.configure("DESCRIBE STREAMS;"),
            SESSION_PROPERTIES,
            engine.getEngine(),
            engine.getServiceContext()
        ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(descriptionList.getSourceDescriptions(), containsInAnyOrder(
        SourceDescriptionFactory.create(
            stream1,
            false,
            ImmutableList.of(),
            ImmutableList.of(),
            Optional.of(topicWith1PartitionAndRfOf1),
            ImmutableList.of(),
            ImmutableList.of("stream2"),
            new MetricCollectors()
        ),
        SourceDescriptionFactory.create(
            stream2,
            false,
            ImmutableList.of(),
            ImmutableList.of(),
            Optional.of(topicWith1PartitionAndRfOf1),
            ImmutableList.of(),
            ImmutableList.of(),
            new MetricCollectors()
        )
    ));
  }

  @Test
  public void shouldShowTables() {
    // Given:
    final KsqlTable<?> table1 = engine.givenSource(DataSourceType.KTABLE, "table1");
    final KsqlTable<?> table2 = engine.givenSource(DataSourceType.KTABLE, "table2");
    engine.givenSource(DataSourceType.KSTREAM, "stream");

    // When:
    final TablesList descriptionList = (TablesList)
        CustomExecutors.LIST_TABLES.execute(
            engine.configure("LIST TABLES;"),
            SESSION_PROPERTIES,
            engine.getEngine(),
            engine.getServiceContext()
        ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(descriptionList.getTables(), containsInAnyOrder(
        new SourceInfo.Table(
            table1.getName().toString(FormatOptions.noEscape()),
            table1.getKsqlTopic().getKafkaTopicName(),
            table2.getKsqlTopic().getKeyFormat().getFormat(),
            table1.getKsqlTopic().getValueFormat().getFormat(),
            table1.getKsqlTopic().getKeyFormat().isWindowed()
        ),
        new SourceInfo.Table(
            table2.getName().toString(FormatOptions.noEscape()),
            table2.getKsqlTopic().getKafkaTopicName(),
            table2.getKsqlTopic().getKeyFormat().getFormat(),
            table2.getKsqlTopic().getValueFormat().getFormat(),
            table2.getKsqlTopic().getKeyFormat().isWindowed()
        )
    ));
  }

  @Test
  public void shouldShowTablesExtended() {
    // Given:
    final KsqlTable<?> table1 = engine.givenSource(DataSourceType.KTABLE, "table1");
    final KsqlTable<?> table2 = engine.givenSource(DataSourceType.KTABLE, "table2",
        ImmutableSet.of(SourceName.of("table1")));
    engine.givenSource(DataSourceType.KSTREAM, "stream");

    // When:
    final SourceDescriptionList descriptionList = (SourceDescriptionList)
        CustomExecutors.LIST_TABLES.execute(
            engine.configure("LIST TABLES EXTENDED;"),
            SESSION_PROPERTIES,
            engine.getEngine(),
            engine.getServiceContext()
        ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    final KafkaTopicClient client = engine.getServiceContext().getTopicClient();
    assertThat(descriptionList.getSourceDescriptions(), containsInAnyOrder(
        SourceDescriptionFactory.create(
            table1,
            true,
            ImmutableList.of(),
            ImmutableList.of(),
            Optional.of(client.describeTopic(table1.getKafkaTopicName())),
            ImmutableList.of(),
            ImmutableList.of("table2"),
            new MetricCollectors()
        ),
        SourceDescriptionFactory.create(
            table2,
            true,
            ImmutableList.of(),
            ImmutableList.of(),
            Optional.of(client.describeTopic(table1.getKafkaTopicName())),
            ImmutableList.of(),
            ImmutableList.of(),
            new MetricCollectors()
        )
    ));
  }

  @Test
  public void shouldDescribeTables() {
    // Given:
    final KsqlTable<?> table1 = engine.givenSource(DataSourceType.KTABLE, "table1");
    final KsqlTable<?> table2 = engine.givenSource(DataSourceType.KTABLE, "table2",
        ImmutableSet.of(SourceName.of("table1")));
    engine.givenSource(DataSourceType.KSTREAM, "stream");

    // When:
    final SourceDescriptionList descriptionList = (SourceDescriptionList)
        CustomExecutors.DESCRIBE_TABLES.execute(
            engine.configure("DESCRIBE TABLES;"),
            SESSION_PROPERTIES,
            engine.getEngine(),
            engine.getServiceContext()
        ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(descriptionList.getSourceDescriptions(), containsInAnyOrder(
        SourceDescriptionFactory.create(
            table1,
            false,
            ImmutableList.of(),
            ImmutableList.of(),
            Optional.of(topicWith1PartitionAndRfOf1),
            ImmutableList.of(),
            ImmutableList.of("table2"),
            new MetricCollectors()
        ),
        SourceDescriptionFactory.create(
            table2,
            false,
            ImmutableList.of(),
            ImmutableList.of(),
            Optional.of(topicWith1PartitionAndRfOf1),
            ImmutableList.of(),
            ImmutableList.of(),
            new MetricCollectors()
        )
    ));
  }

  @Test
  public void shouldShowColumnsSource() {
    // Given:
    engine.givenSource(DataSourceType.KSTREAM, "SOURCE");
    final ExecuteResult result = engine.getEngine().execute(
        engine.getServiceContext(),
        engine.configure("CREATE STREAM SINK AS SELECT * FROM source;")
    );
    final PersistentQueryMetadata metadata = (PersistentQueryMetadata) result.getQuery()
        .orElseThrow(IllegalArgumentException::new);
    final DataSource stream = engine.getEngine().getMetaStore().getSource(SourceName.of("SINK"));

    // When:
    final SourceDescriptionEntity sourceDescription = (SourceDescriptionEntity)
        CustomExecutors.SHOW_COLUMNS.execute(
            ConfiguredStatement.of(PreparedStatement.of(
                "DESCRIBE SINK;",
                new ShowColumns(SourceName.of("SINK"), false)),
                SessionConfig.of(engine.getKsqlConfig(), ImmutableMap.of())),
            SESSION_PROPERTIES,
            engine.getEngine(),
            engine.getServiceContext()
        ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    final QueryStatusCount queryStatusCount =
            new QueryStatusCount(Collections.singletonMap(KsqlConstants.fromStreamsState(metadata.getState()), 1));

    assertThat(
        sourceDescription.getSourceDescription(),
        equalTo(
            SourceDescriptionFactory.create(
                stream,
                false,
                ImmutableList.of(),
                ImmutableList.of(new RunningQuery(
                    metadata.getStatementString(),
                    ImmutableSet.of(
                        metadata.getSinkName().get().toString(FormatOptions.noEscape())),
                    ImmutableSet.of(metadata.getResultTopic().get().getKafkaTopicName()),
                    metadata.getQueryId(),
                    queryStatusCount,
                    KsqlConstants.KsqlQueryType.PERSISTENT)),
                Optional.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                new MetricCollectors()
            )
        )
    );
  }

  @Test
  public void shouldThrowOnDescribeMissingSource() {
    // When:
    final Exception e = assertThrows(
        KsqlStatementException.class,
        () -> CustomExecutors.SHOW_COLUMNS.execute(
            engine.configure("DESCRIBE S;"),
            SESSION_PROPERTIES,
            engine.getEngine(),
            engine.getServiceContext()
        )
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Could not find STREAM/TABLE 'S' in the Metastore"));
  }

  @Test
  public void shouldNotCallTopicClientForExtendedDescription() {
    // Given:
    engine.givenSource(DataSourceType.KSTREAM, "stream1");
    final KafkaTopicClient spyTopicClient = spy(engine.getServiceContext().getTopicClient());
    final ServiceContext serviceContext = TestServiceContext.create(
        engine.getServiceContext().getKafkaClientSupplier(),
        engine.getServiceContext().getAdminClient(),
        spyTopicClient,
        engine.getServiceContext().getSchemaRegistryClientFactory(),
        engine.getServiceContext().getConnectClient()
    );

    // When:
    CustomExecutors.LIST_STREAMS.execute(
        engine.configure("SHOW STREAMS;"),
        SESSION_PROPERTIES,
        engine.getEngine(),
        serviceContext
    ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    verify(spyTopicClient, never()).describeTopic(anyString());
  }

  private static void assertSourceListWithWarning(
      final KsqlEntity entity,
      final DataSource... sources) {
    assertThat(entity, instanceOf(SourceDescriptionList.class));
    final SourceDescriptionList listing = (SourceDescriptionList) entity;
    assertThat(
        listing.getSourceDescriptions(),
        containsInAnyOrder(
            Arrays.stream(sources)
                .map(
                    s -> equalTo(
                        SourceDescriptionFactory.create(
                            s,
                            true,
                            ImmutableList.of(),
                            ImmutableList.of(),
                            Optional.empty(),
                            ImmutableList.of(),
                            ImmutableList.of(),
                            new MetricCollectors()
                        )
                    )
                )
                .collect(Collectors.toList())
        )
    );
    assertThat(
        listing.getWarnings(),
        containsInAnyOrder(
            Arrays.stream(sources)
                .map(
                    s -> equalTo(
                        new KsqlWarning(
                            "Error from Kafka: unknown topic: " + s.getKafkaTopicName())))
                .collect(Collectors.toList())
        )
    );
  }

  @Test
  public void shouldAddWarningsOnClientExceptionForStreamListing() {
    // Given:
    final KsqlStream<?> stream1 = engine.givenSource(DataSourceType.KSTREAM, "stream1");
    final KsqlStream<?> stream2 = engine.givenSource(DataSourceType.KSTREAM, "stream2");
    final ServiceContext serviceContext = engine.getServiceContext();
    serviceContext.getTopicClient().deleteTopics(ImmutableList.of("stream1", "stream2"));

    // When:
    final KsqlEntity entity = CustomExecutors.LIST_STREAMS.execute(
        engine.configure("SHOW STREAMS EXTENDED;"),
        SESSION_PROPERTIES,
        engine.getEngine(),
        serviceContext
    ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    assertSourceListWithWarning(entity, stream1, stream2);
  }

  @Test
  public void shouldAddWarningsOnClientExceptionForTopicListing() {
    // Given:
    final KsqlTable<?> table1 = engine.givenSource(DataSourceType.KTABLE, "table1");
    final KsqlTable<?> table2 = engine.givenSource(DataSourceType.KTABLE, "table2");
    final ServiceContext serviceContext = engine.getServiceContext();
    serviceContext.getTopicClient().deleteTopics(ImmutableList.of("table1", "table2"));

    // When:
    final KsqlEntity entity = CustomExecutors.LIST_TABLES.execute(
        engine.configure("SHOW TABLES EXTENDED;"),
        SESSION_PROPERTIES,
        engine.getEngine(),
        serviceContext
    ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    assertSourceListWithWarning(entity, table1, table2);
  }

  @Test
  public void shouldAddWarningOnClientExceptionForDescription() {
    // Given:
    final KsqlStream<?> stream1 = engine.givenSource(DataSourceType.KSTREAM, "STREAM1");
    final ServiceContext serviceContext = engine.getServiceContext();
    serviceContext.getTopicClient().deleteTopics(ImmutableList.of("STREAM1"));

    // When:
    final KsqlEntity entity = CustomExecutors.SHOW_COLUMNS.execute(
        engine.configure("DESCRIBE STREAM1 EXTENDED;"),
        SESSION_PROPERTIES,
        engine.getEngine(),
        serviceContext
    ).getEntity().orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(entity, instanceOf(SourceDescriptionEntity.class));
    final SourceDescriptionEntity description = (SourceDescriptionEntity) entity;
    assertThat(
        description.getSourceDescription(),
        equalTo(
            SourceDescriptionFactory.create(
                stream1,
                true,
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                new MetricCollectors()
            )
        )
    );
    assertThat(
        description.getWarnings(),
        contains(new KsqlWarning("Error from Kafka: unknown topic: STREAM1")));
  }
}
