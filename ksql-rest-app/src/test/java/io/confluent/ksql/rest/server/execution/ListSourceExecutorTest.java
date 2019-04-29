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

import static io.confluent.ksql.metastore.model.StructuredDataSource.DataSourceType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlExecutionContext.ExecuteResult;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.metastore.model.StructuredDataSource;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.ShowColumns;
import io.confluent.ksql.rest.entity.EntityQueryId;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.SourceDescriptionEntity;
import io.confluent.ksql.rest.entity.SourceDescriptionList;
import io.confluent.ksql.rest.entity.SourceInfo;
import io.confluent.ksql.rest.entity.StreamsList;
import io.confluent.ksql.rest.entity.TablesList;
import io.confluent.ksql.rest.entity.TopicDescription;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
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
        new SourceDescription(stream1, true, "JSON", ImmutableList.of(), ImmutableList.of(), null),
        new SourceDescription(stream2, true, "JSON", ImmutableList.of(), ImmutableList.of(), null)
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
        new SourceDescription(table1, true, "JSON", ImmutableList.of(), ImmutableList.of(), client),
        new SourceDescription(table2, true, "JSON", ImmutableList.of(), ImmutableList.of(), client)
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
    final StructuredDataSource<?> stream = engine.getEngine().getMetaStore().getSource("SINK");

    // When:
    final SourceDescriptionEntity sourceDescription = (SourceDescriptionEntity)
        CustomExecutors.SHOW_COLUMNS.execute(
            ConfiguredStatement.of(
              PreparedStatement.of(
                  "DESCRIBE SINK;",
                  new ShowColumns(QualifiedName.of("SINK"), false, false)),
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
                metadata.getSinkNames(),
                new EntityQueryId(metadata.getQueryId()))),
            null)));
  }

  @Test
  public void shouldShowColumnsTopic() {
    // Given:
    engine.givenSource(DataSourceType.KSTREAM, "S");

    // When:
    final TopicDescription description = (TopicDescription) CustomExecutors.SHOW_COLUMNS.execute(
        engine.configure("DESCRIBE TOPIC S;"),
        engine.getEngine(),
        engine.getServiceContext()
    ).orElseThrow(IllegalStateException::new);

    // Then:
    assertThat(description,
        equalTo(new TopicDescription("DESCRIBE TOPIC S;", "S", "S", "JSON",
            TemporaryEngine.SCHEMA.toString())));
  }

  @Test
  public void shouldThrowOnDescribeMissingTopic() {
    // Expect:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Could not find Topic");

    // When:
    CustomExecutors.SHOW_COLUMNS.execute(
        engine.configure("DESCRIBE TOPIC S;"),
        engine.getEngine(),
        engine.getServiceContext()
    );
  }

}
