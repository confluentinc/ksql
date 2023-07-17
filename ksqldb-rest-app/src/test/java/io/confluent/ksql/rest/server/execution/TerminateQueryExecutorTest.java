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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.StreamsTaskMetadata;
import io.confluent.ksql.rest.entity.TerminateQueryEntity;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsMetadata;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TerminateQueryExecutorTest {

  private static final KafkaStreams.State RUNNING_QUERY_STATE = KafkaStreams.State.RUNNING;

  @Rule public final TemporaryEngine engine = new TemporaryEngine();

  @Test
  public void shouldDefaultToDistributorForPersistentQuery() {
    // Given:
    final ConfiguredStatement<?> terminatePersistent = engine.configure("TERMINATE PERSISTENT_QUERY;");
    final PersistentQueryMetadata persistentQueryMetadata = givenPersistentQuery("PERSISTENT_QUERY", RUNNING_QUERY_STATE);
    final QueryId persistentQueryId= persistentQueryMetadata.getQueryId();

    final KsqlEngine engine = mock(KsqlEngine.class);
    when(engine.getPersistentQuery(persistentQueryId)).thenReturn(Optional.of(persistentQueryMetadata));

    // When:
    final Optional<KsqlEntity> ksqlEntity = CustomExecutors.TERMINATE_QUERY.execute(
        terminatePersistent,
        mock(SessionProperties.class),
        engine,
        this.engine.getServiceContext()
    ).getEntity();

    // Then:
    assertThat(ksqlEntity, is(Optional.empty()));
  }

  @Test
  public void shouldDefaultToDistributorForTerminateCluster() {
    // Given:
    final ConfiguredStatement<?> terminatePersistent = engine.configure("TERMINATE CLUSTER;");
    final KsqlEngine engine = mock(KsqlEngine.class);

    // When:
    final Optional<KsqlEntity> ksqlEntity = CustomExecutors.TERMINATE_QUERY.execute(
        terminatePersistent,
        mock(SessionProperties.class),
        engine,
        this.engine.getServiceContext()
    ).getEntity();

    // Then:
    assertThat(ksqlEntity, is(Optional.empty()));
  }

  @Test
  public void shouldDefaultToDistributorForTerminateAll() {
    // Given:
    final ConfiguredStatement<?> terminatePersistent = engine.configure("TERMINATE ALL;");
    final KsqlEngine engine = mock(KsqlEngine.class);

    // When:
    final Optional<KsqlEntity> ksqlEntity = CustomExecutors.TERMINATE_QUERY.execute(
        terminatePersistent,
        mock(SessionProperties.class),
        engine,
        this.engine.getServiceContext()
    ).getEntity();

    // Then:
    assertThat(ksqlEntity, is(Optional.empty()));
  }

  @Test
  public void shouldTerminateTransientQuery() {
    // Given:
    final ConfiguredStatement<?> terminateTransient= engine.configure("TERMINATE TRANSIENT_QUERY;");
    final TransientQueryMetadata transientQueryMetadata = givenTransientQuery("TRANSIENT_QUERY", RUNNING_QUERY_STATE);
    final QueryId transientQueryId= transientQueryMetadata.getQueryId();

    final KsqlEngine engine = mock(KsqlEngine.class);
    when(engine.getQuery(transientQueryId)).thenReturn(
        Optional.of(transientQueryMetadata));

    // When:
    final Optional<KsqlEntity> ksqlEntity = CustomExecutors.TERMINATE_QUERY.execute(
        terminateTransient,
        mock(SessionProperties.class),
        engine,
        this.engine.getServiceContext()
    ).getEntity();

    // Then:
    assertThat(ksqlEntity, is(Optional.of(new TerminateQueryEntity(terminateTransient.getMaskedStatementText(), transientQueryId.toString(), true))));
  }

  @Test
  public void shouldFailToTerminateTransientQuery() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> CustomExecutors.TERMINATE_QUERY.execute(
            engine.configure("TERMINATE TRANSIENT_QUERY;"),
            mock(SessionProperties.class),
            engine.getEngine(),
            this.engine.getServiceContext()
        ));

    // Then:
    assertThat(e.getMessage(), containsString(
        "Failed to terminate query with query ID: 'TRANSIENT_QUERY'"));
  }

  public static PersistentQueryMetadata givenPersistentQuery(
      final String id,
      final KafkaStreams.State state
  ) {
    return givenPersistentQuery(id, state, Collections.emptySet());
  }

  public static PersistentQueryMetadata givenPersistentQuery(
      final String id,
      final KafkaStreams.State state,
      final Set<StreamsTaskMetadata> tasksMetadata
  ) {
    final PersistentQueryMetadata metadata = mock(PersistentQueryMetadata.class);
    mockQuery(id, state, metadata);
    return metadata;
  }

  public static TransientQueryMetadata givenTransientQuery(
      final String id,
      final KafkaStreams.State state
  ) {
    final TransientQueryMetadata metadata = mock(TransientQueryMetadata.class);
    mockQuery(id, state, metadata);

    return metadata;
  }

  @SuppressWarnings("SameParameterValue")
  public static void mockQuery(
      final String id,
      final KafkaStreams.State state,
      final QueryMetadata metadata
  ) {
    when(metadata.getQueryId()).thenReturn(new QueryId(id));

    final StreamsMetadata localStreamsMetadata = mock(StreamsMetadata.class);
    final StreamsMetadata remoteStreamsMetadata = mock(StreamsMetadata.class);
    final List<StreamsMetadata> streamsData = new ArrayList<>();
    streamsData.add(localStreamsMetadata);
    streamsData.add(remoteStreamsMetadata);
  }
}
