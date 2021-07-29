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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.client.KsqlRestClientException;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.QueryDescription;
import io.confluent.ksql.rest.entity.QueryDescriptionFactory;
import io.confluent.ksql.rest.entity.QueryDescriptionList;
import io.confluent.ksql.rest.entity.QueryStatusCount;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.entity.StreamsTaskMetadata;
import io.confluent.ksql.rest.entity.TerminateQueryEntity;
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.rest.server.computation.DistributingExecutor;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryStatus;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TerminateQueryExecutorTest {

  private static final HostInfo REMOTE_HOST = new HostInfo("otherhost", 1234);
  private static final HostInfo LOCAL_HOST = new HostInfo("HOST", 444);
  private static final KsqlHostInfoEntity LOCAL_KSQL_HOST_INFO_ENTITY =
      new KsqlHostInfoEntity(LOCAL_HOST.host(), LOCAL_HOST.port());
  private static final KafkaStreams.State RUNNING_QUERY_STATE = KafkaStreams.State.RUNNING;

  @Rule public final TemporaryEngine engine = new TemporaryEngine();

  @Mock
  private SessionProperties sessionProperties;
  @Mock
  private RestResponse<KsqlEntityList> response;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private SimpleKsqlClient ksqlClient;
  @Mock
  private DistributingExecutor distributingExecutor;
  @Mock
  private KsqlSecurityContext ksqlSecurityContext;

  @Before
  public void setup() throws MalformedURLException {
    // set to true so the tests don't perform the scatter gather by default
    when(sessionProperties.getInternalRequest()).thenReturn(true);
    when(sessionProperties.getKsqlHostInfo()).thenReturn(LOCAL_KSQL_HOST_INFO_ENTITY.toKsqlHost());
    when(sessionProperties.getLocalUrl()).thenReturn(new URL("https://address"));

    when(ksqlClient.makeKsqlRequest(any(), any(), any())).thenReturn(response);
    when(response.isErroneous()).thenReturn(false);
    when(serviceContext.getKsqlClient()).thenReturn(ksqlClient);
  }

  @Test
  public void shouldDefaultToDistributorForPersistentQuery() {
    // Given:
    final ConfiguredStatement<?> terminatePersistent = engine.configure("TERMINATE PERSISTENT_QUERY;");
    final PersistentQueryMetadata persistentQueryMetadata = givenPersistentQuery("PERSISTENT_QUERY", RUNNING_QUERY_STATE);
    final QueryId persistentQueryId= persistentQueryMetadata.getQueryId();

    final KsqlEngine engine = mock(KsqlEngine.class);
    when(engine.getPersistentQuery(persistentQueryId)).thenReturn(Optional.of(persistentQueryMetadata));

    final Optional<KsqlEntity> ksqlEntity = CustomExecutors.TERMINATE_QUERY.execute(
        terminatePersistent,
        mock(SessionProperties.class),
        engine,
        this.engine.getServiceContext(),
        distributingExecutor,
        ksqlSecurityContext
    );

    // Then:
    assertThat(ksqlEntity, is(Optional.empty()));
  }

  @Test
  public void shouldTerminateTransientQueryLocally() {
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
        this.engine.getServiceContext(),
        distributingExecutor,
        ksqlSecurityContext
    );

    // Then:
    assertThat(ksqlEntity, is(Optional.of(new TerminateQueryEntity(terminateTransient.getStatementText(), transientQueryId.toString(), true))));
  }

  @Test
  public void shouldFailToTerminateTransientQueryLocally() {
    // Given:
    when(sessionProperties.getInternalRequest()).thenReturn(false);

    final ConfiguredStatement<?> terminateTransient= engine.configure("TERMINATE TRANSIENT_QUERY;");
    final TransientQueryMetadata transientQueryMetadata = givenTransientQuery("TRANSIENT_QUERY", RUNNING_QUERY_STATE);
    final QueryId transientQueryId= transientQueryMetadata.getQueryId();

    // When:
    final Optional<KsqlEntity> ksqlEntity = CustomExecutors.TERMINATE_QUERY.execute(
        terminateTransient,
        mock(SessionProperties.class),
        engine.getEngine(),
        this.engine.getServiceContext(),
        distributingExecutor,
        ksqlSecurityContext
    );

    // Then:
    assertThat(ksqlEntity, is(Optional.of(new TerminateQueryEntity(terminateTransient.getStatementText(), transientQueryId.toString(), false))));
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
    when(metadata.getQueryType()).thenReturn(KsqlConstants.KsqlQueryType.PERSISTENT);
    when(metadata.getSinkName()).thenReturn(SourceName.of(id));
    final KsqlTopic sinkTopic = mock(KsqlTopic.class);
    when(sinkTopic.getKeyFormat()).thenReturn(
        KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name()), SerdeFeatures.of()));
    when(sinkTopic.getKafkaTopicName()).thenReturn(id);
    when(metadata.getResultTopic()).thenReturn(sinkTopic);
    when(metadata.getTaskMetadata()).thenReturn(tasksMetadata);

    return metadata;
  }

  public static TransientQueryMetadata givenTransientQuery(
      final String id,
      final KafkaStreams.State state
  ) {
    final TransientQueryMetadata metadata = mock(TransientQueryMetadata.class);
    mockQuery(id, state, metadata);
    when(metadata.getQueryType()).thenReturn(KsqlConstants.KsqlQueryType.PUSH);

    return metadata;
  }

  @SuppressWarnings("SameParameterValue")
  public static void mockQuery(
      final String id,
      final KafkaStreams.State state,
      final QueryMetadata metadata
  ) {
    when(metadata.getStatementString()).thenReturn("sql");
    when(metadata.getQueryId()).thenReturn(new QueryId(id));
    when(metadata.getLogicalSchema()).thenReturn(TemporaryEngine.SCHEMA);
    when(metadata.getState()).thenReturn(state);
    when(metadata.getTopologyDescription()).thenReturn("topology");
    when(metadata.getExecutionPlan()).thenReturn("plan");

    final StreamsMetadata localStreamsMetadata = mock(StreamsMetadata.class);
    when(localStreamsMetadata.hostInfo()).thenReturn(LOCAL_HOST);
    final StreamsMetadata remoteStreamsMetadata = mock(StreamsMetadata.class);
    when(remoteStreamsMetadata.hostInfo()).thenReturn(REMOTE_HOST);
    final List<StreamsMetadata> streamsData = new ArrayList<>();
    streamsData.add(localStreamsMetadata);
    streamsData.add(remoteStreamsMetadata);
    when(metadata.getAllMetadata()).thenReturn(streamsData);
  }
}
