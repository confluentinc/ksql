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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.rest.entity.EntityQueryId;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlWarning;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.SourceDescriptionEntity;
import io.confluent.ksql.rest.entity.SourceDescriptionList;
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.TestServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlStatementException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ListSourceExecutorTest {

  @Rule
  public final TemporaryEngine engine = new TemporaryEngine();
  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

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
            engine.getEngine(),
            engine.getServiceContext()
        ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(descriptionList.getStreams(), containsInAnyOrder(
        new SourceInfo.Stream(stream1),
        new SourceInfo.Stream(stream2)
    ));
  }

  @Test
  public void shouldShowStreamsExtended() {
    // Given:
    final KsqlStream<?> stream1 = engine.givenSource(DataSourceType.KSTREAM, "stream1");
    final KsqlStream<?> stream2 = engine.givenSource(DataSourceType.KSTREAM, "stream2");
    engine.givenSource(DataSourceType.KTABLE, "table");

    // When:
    final SourceDescriptionList descriptionList = (SourceDescriptionList)
        CustomExecutors.LIST_STREAMS.execute(
            engine.configure("SHOW STREAMS EXTENDED;"),
            engine.getEngine(),
            engine.getServiceContext()
        ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(descriptionList.getSourceDescriptions(), containsInAnyOrder(
        new SourceDescription(
            stream1,
            true,
            "JSON",
            ImmutableList.of(),
            ImmutableList.of(),
            Optional.empty()),
        new SourceDescription(
            stream2,
            true,
            "JSON",
            ImmutableList.of(),
            ImmutableList.of(),
            Optional.empty())
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
            engine.getEngine(),
            engine.getServiceContext()
        ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(descriptionList.getTables(), containsInAnyOrder(
        new SourceInfo.Table(table1),
        new SourceInfo.Table(table2)
    ));
  }

  @Test
  public void shouldShowTablesExtended() {
    // Given:
    final KsqlTable<?> table1 = engine.givenSource(DataSourceType.KTABLE, "table1");
    final KsqlTable<?> table2 = engine.givenSource(DataSourceType.KTABLE, "table2");
    engine.givenSource(DataSourceType.KSTREAM, "stream");

    // When:
    final SourceDescriptionList descriptionList = (SourceDescriptionList)
        CustomExecutors.LIST_TABLES.execute(
            engine.configure("LIST TABLES EXTENDED;"),
            engine.getEngine(),
            engine.getServiceContext()
        ).orElseThrow(IllegalStateException::new);

    // Then:
    final KafkaTopicClient client = engine.getServiceContext().getTopicClient();
    assertThat(descriptionList.getSourceDescriptions(), containsInAnyOrder(
        new SourceDescription(
            table1,
            true,
            "JSON",
            ImmutableList.of(),
            ImmutableList.of(),
            Optional.of(client.describeTopic(table1.getKafkaTopicName()))
        ),
        new SourceDescription(
            table2,
            true,
            "JSON",
            ImmutableList.of(),
            ImmutableList.of(),
            Optional.of(client.describeTopic(table1.getKafkaTopicName()))
        )
    ));
  }

  @Test
  public void shouldShowColumnsSource() {
    // Given:
    engine.givenSource(DataSourceType.KSTREAM, "SOURCE");
    final ExecuteResult result = engine.getEngine().execute(
        engine.configure("CREATE STREAM SINK AS SELECT * FROM source;")
    );
    final PersistentQueryMetadata metadata = (PersistentQueryMetadata) result.getQuery()
        .orElseThrow(IllegalArgumentException::new);
    final DataSource<?> stream = engine.getEngine().getMetaStore().getSource("SINK");

    // When:
    final SourceDescriptionEntity sourceDescription = (SourceDescriptionEntity)
        CustomExecutors.SHOW_COLUMNS.execute(
            ConfiguredStatement.of(
              PreparedStatement.of(
                  "DESCRIBE SINK;",
                  new ShowColumns(QualifiedName.of("SINK"), false)),
                ImmutableMap.of(),
                engine.getKsqlConfig()
            ),
            engine.getEngine(),
            engine.getServiceContext()
        ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(sourceDescription.getSourceDescription(),
        equalTo(new SourceDescription(
            stream,
            false,
            "JSON",
            ImmutableList.of(),
            ImmutableList.of(new RunningQuery(
                metadata.getStatementString(),
                ImmutableSet.of(metadata.getSinkName()),
                new EntityQueryId(metadata.getQueryId()))),
            Optional.empty())));
  }

  @Test
  public void shouldThrowOnDescribeMissingSource() {
    // Expect:
    expectedException.expect(KsqlStatementException.class);
    expectedException.expectMessage("Could not find STREAM/TABLE 'S' in the Metastore");

    // When:
    CustomExecutors.SHOW_COLUMNS.execute(
        engine.configure("DESCRIBE S;"),
        engine.getEngine(),
        engine.getServiceContext()
    );
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
        engine.getEngine(),
        serviceContext
    ).orElseThrow(IllegalStateException::new);

    // Then:
    verify(spyTopicClient, never()).describeTopic(anyString());
  }

  private void assertSourceListWithWarning(
      final KsqlEntity entity,
      final DataSource<?>... sources) {
    assertThat(entity, instanceOf(SourceDescriptionList.class));
    final SourceDescriptionList listing = (SourceDescriptionList) entity;
    assertThat(
        listing.getSourceDescriptions(),
        containsInAnyOrder(
            Arrays.stream(sources)
                .map(
                    s -> equalTo(
                        new SourceDescription(
                            s,
                            true,
                            "JSON",
                            ImmutableList.of(),
                            ImmutableList.of(),
                            Optional.empty()
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
        engine.getEngine(),
        serviceContext
    ).orElseThrow(IllegalStateException::new);

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
        engine.getEngine(),
        serviceContext
    ).orElseThrow(IllegalStateException::new);

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
        engine.configure("DESCRIBE EXTENDED STREAM1;"),
        engine.getEngine(),
        serviceContext
    ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(entity, instanceOf(SourceDescriptionEntity.class));
    final SourceDescriptionEntity description = (SourceDescriptionEntity) entity;
    assertThat(
        description.getSourceDescription(),
        equalTo(
            new SourceDescription(
                stream1,
                true,
                "JSON",
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty()
            )
        )
    );
    assertThat(
        description.getWarnings(),
        contains(new KsqlWarning("Error from Kafka: unknown topic: STREAM1")));
  }
}
