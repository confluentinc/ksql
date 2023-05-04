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
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
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
import io.confluent.ksql.rest.server.TemporaryEngine;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.SimpleKsqlClient;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
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
public class ListQueriesExecutorTest {

  private static final HostInfo REMOTE_HOST = new HostInfo("otherhost", 1234);
  private static final HostInfo LOCAL_HOST = new HostInfo("HOST", 444);
  private static final KsqlHostInfoEntity LOCAL_KSQL_HOST_INFO_ENTITY =
      new KsqlHostInfoEntity(LOCAL_HOST.host(), LOCAL_HOST.port());
  private static final KsqlHostInfoEntity REMOTE_KSQL_HOST_INFO_ENTITY =
      new KsqlHostInfoEntity(REMOTE_HOST.host(), REMOTE_HOST.port());

  private static final KafkaStreams.State RUNNING_QUERY_STATE = KafkaStreams.State.RUNNING;
  private static final KafkaStreams.State ERROR_QUERY_STATE = KafkaStreams.State.ERROR;

  private static final Map<KsqlHostInfoEntity, KsqlQueryStatus> LOCAL_KSQL_HOST_INFO_MAP =
      Collections.singletonMap(LOCAL_KSQL_HOST_INFO_ENTITY, KsqlQueryStatus.RUNNING);
  
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
  private KsqlEntityList ksqlEntityList;
  @Mock
  private Queries remoteQueries;
  @Mock
  private QueryDescriptionList remoteQueryDescriptionList;
  @Mock
  private KsqlEngine mockKsqlEngine;
  @Mock 
  private KsqlConfig ksqlConfig;
  private QueryStatusCount queryStatusCount;

  @Before
  public void setup() throws MalformedURLException {
    // set to true so the tests don't perform the scatter gather by default
    when(sessionProperties.getInternalRequest()).thenReturn(true);
    when(sessionProperties.getKsqlHostInfo()).thenReturn(LOCAL_KSQL_HOST_INFO_ENTITY.toKsqlHost());
    when(sessionProperties.getLocalUrl()).thenReturn(new URL("https://address"));

    queryStatusCount = new QueryStatusCount();

    when(ksqlClient.makeKsqlRequest(any(), any(), any())).thenReturn(response);
    when(response.isErroneous()).thenReturn(false);
    when(serviceContext.getKsqlClient()).thenReturn(ksqlClient);
    when(mockKsqlEngine.getKsqlConfig()).thenReturn(ksqlConfig);
    when(ksqlConfig.getLong(KsqlConfig.KSQL_FETCH_REMOTE_HOSTS_TIMEOUT_SECONDS)).thenReturn(KsqlConfig.KSQL_FETCH_REMOTE_HOSTS_TIMEOUT_SECONDS_DEFAULT);
  }

  @Test
  public void shouldListQueriesEmpty() {
    // When
    final Queries queries = (Queries) CustomExecutors.LIST_QUERIES.execute(
        engine.configure("SHOW QUERIES;"),
        mock(SessionProperties.class),
        engine.getEngine(),
        engine.getServiceContext()
    ).getEntity().orElseThrow(IllegalStateException::new);

    assertThat(queries.getQueries(), is(empty()));
  }

  @Test
  public void shouldListQueriesBasic() {
    // Given
    final ConfiguredStatement<?> showQueries = engine.configure("SHOW QUERIES;");
    final PersistentQueryMetadata metadata = givenPersistentQuery("id", RUNNING_QUERY_STATE);

    
    when(mockKsqlEngine.getAllLiveQueries()).thenReturn(ImmutableList.of(metadata));
    queryStatusCount.updateStatusCount(RUNNING_QUERY_STATE, 1);

    // When
    final Queries queries = (Queries) CustomExecutors.LIST_QUERIES.execute(
        showQueries,
        sessionProperties,
        mockKsqlEngine,
        this.engine.getServiceContext()
    ).getEntity().orElseThrow(IllegalStateException::new);

    assertThat(queries.getQueries(), containsInAnyOrder(persistentQueryMetadataToRunningQuery(metadata, queryStatusCount)));
  }

  @Test
  public void shouldScatterGatherAndMergeShowQueries() {
    // Given
    when(sessionProperties.getInternalRequest()).thenReturn(false);
    final ConfiguredStatement<?> showQueries = engine.configure("SHOW QUERIES;");
    final PersistentQueryMetadata localMetadata = givenPersistentQuery("id", RUNNING_QUERY_STATE);
    final PersistentQueryMetadata remoteMetadata = givenPersistentQuery("id", ERROR_QUERY_STATE);

    
    when(mockKsqlEngine.getAllLiveQueries()).thenReturn(ImmutableList.of(localMetadata));
    when(mockKsqlEngine.getPersistentQueries()).thenReturn(ImmutableList.of(localMetadata));

    final List<RunningQuery> remoteRunningQueries = Collections.singletonList(persistentQueryMetadataToRunningQuery(
        remoteMetadata,
            new QueryStatusCount(Collections.singletonMap(KsqlQueryStatus.ERROR, 1))));


    when(remoteQueries.getQueries()).thenReturn(remoteRunningQueries);
    when(ksqlEntityList.get(anyInt())).thenReturn(remoteQueries);
    when(response.getResponse()).thenReturn(ksqlEntityList);

    queryStatusCount.updateStatusCount(RUNNING_QUERY_STATE, 1);
    queryStatusCount.updateStatusCount(ERROR_QUERY_STATE, 1);

    // When
    final Queries queries = (Queries) CustomExecutors.LIST_QUERIES.execute(
        showQueries,
        sessionProperties,
        mockKsqlEngine,
        serviceContext
    ).getEntity().orElseThrow(IllegalStateException::new);

    // Then
    assertThat(queries.getQueries(), containsInAnyOrder(persistentQueryMetadataToRunningQuery(localMetadata, queryStatusCount)));
  }

  @Test
  public void shouldNotMergeDifferentRunningQueries() {
    // Given
    when(sessionProperties.getInternalRequest()).thenReturn(false);
    final ConfiguredStatement<?> showQueries = engine.configure("SHOW QUERIES;");
    final PersistentQueryMetadata localMetadata = givenPersistentQuery("id", RUNNING_QUERY_STATE);
    final PersistentQueryMetadata remoteMetadata = givenPersistentQuery("different Id", RUNNING_QUERY_STATE);

    
    when(mockKsqlEngine.getAllLiveQueries()).thenReturn(ImmutableList.of(localMetadata));
    when(mockKsqlEngine.getPersistentQueries()).thenReturn(ImmutableList.of(localMetadata));
    
    final List<RunningQuery> remoteRunningQueries = Collections.singletonList(persistentQueryMetadataToRunningQuery(
        remoteMetadata,
            new QueryStatusCount(Collections.singletonMap(KsqlQueryStatus.RUNNING, 1))));
    when(remoteQueries.getQueries()).thenReturn(remoteRunningQueries);
    when(ksqlEntityList.get(anyInt())).thenReturn(remoteQueries);
    when(response.getResponse()).thenReturn(ksqlEntityList);

    queryStatusCount.updateStatusCount(RUNNING_QUERY_STATE, 1);

    // When
    final Queries queries = (Queries) CustomExecutors.LIST_QUERIES.execute(
        showQueries,
        sessionProperties,
        mockKsqlEngine,
        serviceContext
    ).getEntity().orElseThrow(IllegalStateException::new);
    
    // Then
    assertThat(queries.getQueries(),
        containsInAnyOrder(
            persistentQueryMetadataToRunningQuery(localMetadata, queryStatusCount),
            persistentQueryMetadataToRunningQuery(remoteMetadata, queryStatusCount)));
  }

  @Test
  public void shouldIncludeUnresponsiveIfShowQueriesFutureThrowsException() {
    // Given
    when(sessionProperties.getInternalRequest()).thenReturn(false);
    final ConfiguredStatement<?> showQueries = engine.configure("SHOW QUERIES;");
    final PersistentQueryMetadata metadata = givenPersistentQuery("id", RUNNING_QUERY_STATE);

    
    when(mockKsqlEngine.getAllLiveQueries()).thenReturn(ImmutableList.of(metadata));
    when(mockKsqlEngine.getPersistentQueries()).thenReturn(ImmutableList.of(metadata));

    when(ksqlClient.makeKsqlRequest(any(), any(), any())).thenThrow(new KsqlRestClientException("error"));
    when(serviceContext.getKsqlClient()).thenReturn(ksqlClient);
    queryStatusCount.updateStatusCount(RUNNING_QUERY_STATE, 1);
    queryStatusCount.updateStatusCount(KsqlQueryStatus.UNRESPONSIVE, 1);

    // When
    final Queries queries = (Queries) CustomExecutors.LIST_QUERIES.execute(
        showQueries,
        sessionProperties,
        mockKsqlEngine,
        serviceContext
    ).getEntity().orElseThrow(IllegalStateException::new);

    // Then
    assertThat(queries.getQueries(), containsInAnyOrder(persistentQueryMetadataToRunningQuery(metadata, queryStatusCount)));
  }

  @Test
  public void shouldIncludeUnresponsiveIfShowQueriesErrorResponse() {
    // Given
    when(sessionProperties.getInternalRequest()).thenReturn(false);
    final ConfiguredStatement<?> showQueries = engine.configure("SHOW QUERIES;");
    final PersistentQueryMetadata metadata = givenPersistentQuery("id", RUNNING_QUERY_STATE);

    
    when(mockKsqlEngine.getAllLiveQueries()).thenReturn(ImmutableList.of(metadata));
    when(mockKsqlEngine.getPersistentQueries()).thenReturn(ImmutableList.of(metadata));

    when(response.isErroneous()).thenReturn(true);
    when(response.getErrorMessage()).thenReturn(new KsqlErrorMessage(10000, "error"));
    queryStatusCount.updateStatusCount(RUNNING_QUERY_STATE, 1);
    queryStatusCount.updateStatusCount(KsqlQueryStatus.UNRESPONSIVE, 1);

    // When
    final Queries queries = (Queries) CustomExecutors.LIST_QUERIES.execute(
        showQueries,
        sessionProperties,
        mockKsqlEngine,
        serviceContext
    ).getEntity().orElseThrow(IllegalStateException::new);

    // Then
    assertThat(queries.getQueries(), containsInAnyOrder(persistentQueryMetadataToRunningQuery(metadata, queryStatusCount)));
  }

  @Test
  public void shouldListQueriesExtended() {
    // Given
    final ConfiguredStatement<?> showQueries = engine.configure("SHOW QUERIES EXTENDED;");
    final PersistentQueryMetadata metadata = givenPersistentQuery("id", KafkaStreams.State.CREATED);

    
    when(mockKsqlEngine.getAllLiveQueries()).thenReturn(ImmutableList.of(metadata));

    // When
    final QueryDescriptionList queries = (QueryDescriptionList) CustomExecutors.LIST_QUERIES.execute(
        showQueries,
        sessionProperties,
        mockKsqlEngine,
        this.engine.getServiceContext()
    ).getEntity().orElseThrow(IllegalStateException::new);

    // Then
    assertThat(queries.getQueryDescriptions(), containsInAnyOrder(
        QueryDescriptionFactory.forQueryMetadata(metadata, LOCAL_KSQL_HOST_INFO_MAP)));
  }

  @Test
  public void shouldScatterGatherAndMergeShowQueriesExtended() {
    // Given
    when(sessionProperties.getInternalRequest()).thenReturn(false);
    final ConfiguredStatement<?> showQueries = engine.configure("SHOW QUERIES EXTENDED;");
    
    final StreamsTaskMetadata localTaskMetadata = mock(StreamsTaskMetadata.class);
    final StreamsTaskMetadata remoteTaskMetadata = mock(StreamsTaskMetadata.class);
    final PersistentQueryMetadata localMetadata = givenPersistentQuery("id", RUNNING_QUERY_STATE, Collections.singleton(localTaskMetadata));
    final PersistentQueryMetadata remoteMetadata = givenPersistentQuery("id", ERROR_QUERY_STATE, Collections.singleton(remoteTaskMetadata));

    
    when(mockKsqlEngine.getAllLiveQueries()).thenReturn(ImmutableList.of(localMetadata));
    when(mockKsqlEngine.getPersistentQueries()).thenReturn(ImmutableList.of(localMetadata));

    final List<QueryDescription> remoteQueryDescriptions = Collections.singletonList(
        QueryDescriptionFactory.forQueryMetadata(
            remoteMetadata, 
            Collections.singletonMap(REMOTE_KSQL_HOST_INFO_ENTITY, KsqlQueryStatus.ERROR))
    );
    when(remoteQueryDescriptionList.getQueryDescriptions()).thenReturn(remoteQueryDescriptions);
    when(ksqlEntityList.get(anyInt())).thenReturn(remoteQueryDescriptionList);
    when(response.getResponse()).thenReturn(ksqlEntityList);
    
    final Map<KsqlHostInfoEntity, KsqlQueryStatus> mergedMap = new HashMap<>();
    mergedMap.put(REMOTE_KSQL_HOST_INFO_ENTITY, KsqlQueryStatus.ERROR);
    mergedMap.put(LOCAL_KSQL_HOST_INFO_ENTITY, KsqlQueryStatus.RUNNING);

    // When
    final QueryDescriptionList queries = (QueryDescriptionList) CustomExecutors.LIST_QUERIES.execute(
        showQueries,
        sessionProperties,
        mockKsqlEngine,
        serviceContext
    ).getEntity().orElseThrow(IllegalStateException::new);

    // Then
    final QueryDescription mergedQueryDescription = QueryDescriptionFactory.forQueryMetadata(localMetadata, mergedMap);
    mergedQueryDescription.updateTaskMetadata(Collections.singleton(remoteTaskMetadata));
    assertThat(queries.getQueryDescriptions(), 
        containsInAnyOrder(mergedQueryDescription));
  }

  @Test
  public void shouldNotMergeDifferentQueryDescriptions() {
    // Given
    when(sessionProperties.getInternalRequest()).thenReturn(false);
    final ConfiguredStatement<?> showQueries = engine.configure("SHOW QUERIES EXTENDED;");
    final PersistentQueryMetadata localMetadata = givenPersistentQuery("id", RUNNING_QUERY_STATE);
    final PersistentQueryMetadata remoteMetadata = givenPersistentQuery("different id", ERROR_QUERY_STATE);

    
    when(mockKsqlEngine.getAllLiveQueries()).thenReturn(ImmutableList.of(localMetadata));
    when(mockKsqlEngine.getPersistentQueries()).thenReturn(ImmutableList.of(localMetadata));

    final Map<KsqlHostInfoEntity, KsqlQueryStatus> remoteMap = Collections.singletonMap(REMOTE_KSQL_HOST_INFO_ENTITY, KsqlQueryStatus.RUNNING);
    final List<QueryDescription> remoteQueryDescriptions = Collections.singletonList(
        QueryDescriptionFactory.forQueryMetadata(
            remoteMetadata,
            remoteMap)
    );
    when(remoteQueryDescriptionList.getQueryDescriptions()).thenReturn(remoteQueryDescriptions);
    when(ksqlEntityList.get(anyInt())).thenReturn(remoteQueryDescriptionList);
    when(response.getResponse()).thenReturn(ksqlEntityList);

    // When
    final QueryDescriptionList queries = (QueryDescriptionList) CustomExecutors.LIST_QUERIES.execute(
        showQueries,
        sessionProperties,
        mockKsqlEngine,
        serviceContext
    ).getEntity().orElseThrow(IllegalStateException::new);

    // Then
    assertThat(queries.getQueryDescriptions(),
        containsInAnyOrder(
            QueryDescriptionFactory.forQueryMetadata(localMetadata, LOCAL_KSQL_HOST_INFO_MAP),
            QueryDescriptionFactory.forQueryMetadata(remoteMetadata, remoteMap)));
  }

  @Test
  public void shouldIncludeUnresponsiveIfShowQueriesExtendedFutureThrowsException() {
    // Given
    when(sessionProperties.getInternalRequest()).thenReturn(false);
    final ConfiguredStatement<?> showQueries = engine.configure("SHOW QUERIES EXTENDED;");
    final PersistentQueryMetadata metadata = givenPersistentQuery("id", RUNNING_QUERY_STATE);

    
    when(mockKsqlEngine.getAllLiveQueries()).thenReturn(ImmutableList.of(metadata));
    when(mockKsqlEngine.getPersistentQueries()).thenReturn(ImmutableList.of(metadata));

    when(ksqlClient.makeKsqlRequest(any(), any(), any())).thenThrow(new KsqlRestClientException("error"));
    when(serviceContext.getKsqlClient()).thenReturn(ksqlClient);

    // When
    final Map<KsqlHostInfoEntity, KsqlQueryStatus> map = new HashMap<>();
    map.put(LOCAL_KSQL_HOST_INFO_ENTITY, KsqlQueryStatus.RUNNING);
    map.put(REMOTE_KSQL_HOST_INFO_ENTITY, KsqlQueryStatus.UNRESPONSIVE);

    // When
    final QueryDescriptionList queries = (QueryDescriptionList) CustomExecutors.LIST_QUERIES.execute(
        showQueries,
        sessionProperties,
        mockKsqlEngine,
        serviceContext
    ).getEntity().orElseThrow(IllegalStateException::new);

    // Then
    assertThat(queries.getQueryDescriptions(),
        containsInAnyOrder(QueryDescriptionFactory.forQueryMetadata(metadata, map)));
  }

  @Test
  public void shouldIncludeUnresponsiveIfShowQueriesExtendedErrorResponse() {
    // Given
    when(sessionProperties.getInternalRequest()).thenReturn(false);
    final ConfiguredStatement<?> showQueries = engine.configure("SHOW QUERIES EXTENDED;");
    final PersistentQueryMetadata metadata = givenPersistentQuery("id", RUNNING_QUERY_STATE);

    
    when(mockKsqlEngine.getAllLiveQueries()).thenReturn(ImmutableList.of(metadata));
    when(mockKsqlEngine.getPersistentQueries()).thenReturn(ImmutableList.of(metadata));

    when(response.isErroneous()).thenReturn(true);
    when(response.getErrorMessage()).thenReturn(new KsqlErrorMessage(10000, "error"));

    final Map<KsqlHostInfoEntity, KsqlQueryStatus> map = new HashMap<>();
    map.put(LOCAL_KSQL_HOST_INFO_ENTITY, KsqlQueryStatus.RUNNING);
    map.put(REMOTE_KSQL_HOST_INFO_ENTITY, KsqlQueryStatus.UNRESPONSIVE);

    // When
    final QueryDescriptionList queries = (QueryDescriptionList) CustomExecutors.LIST_QUERIES.execute(
        showQueries,
        sessionProperties,
        mockKsqlEngine,
        serviceContext
    ).getEntity().orElseThrow(IllegalStateException::new);

    // Then
    assertThat(queries.getQueryDescriptions(),
        containsInAnyOrder(QueryDescriptionFactory.forQueryMetadata(metadata, map)));
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
    when(metadata.getSinkName()).thenReturn(Optional.of(SourceName.of(id)));
    final KsqlTopic sinkTopic = mock(KsqlTopic.class);
    when(sinkTopic.getKeyFormat()).thenReturn(
        KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name()), SerdeFeatures.of()));
    when(sinkTopic.getKafkaTopicName()).thenReturn(id);
    when(metadata.getResultTopic()).thenReturn(Optional.of(sinkTopic));
    when(metadata.getTaskMetadata()).thenReturn(tasksMetadata);
    when(metadata.getQueryStatus()).thenReturn(KsqlConstants.fromStreamsState(state));

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
    when(metadata.getQueryApplicationId()).thenReturn("consumer-group-id");

    final StreamsMetadata localStreamsMetadata = mock(StreamsMetadata.class);
    when(localStreamsMetadata.hostInfo()).thenReturn(LOCAL_HOST);
    final StreamsMetadata remoteStreamsMetadata = mock(StreamsMetadata.class);
    when(remoteStreamsMetadata.hostInfo()).thenReturn(REMOTE_HOST);
    final List<StreamsMetadata> streamsData = new ArrayList<>();
    streamsData.add(localStreamsMetadata);
    streamsData.add(remoteStreamsMetadata);
    when(metadata.getAllStreamsHostMetadata()).thenReturn(streamsData);
  }

  public static RunningQuery persistentQueryMetadataToRunningQuery(
      final PersistentQueryMetadata md,
      final QueryStatusCount queryStatusCount
  ) {
    return new RunningQuery(
        md.getStatementString(),
        md.getSinkName().isPresent()
            ? ImmutableSet.of(md.getSinkName().get().text())
            : ImmutableSet.of(),
        md.getResultTopic().isPresent()
            ? ImmutableSet.of(md.getResultTopic().get().getKafkaTopicName())
            : ImmutableSet.of(),
        md.getQueryId(),
        queryStatusCount,
        md.getQueryType());
  }

  public static RunningQuery transientQueryMetadataToRunningQuery(
      final TransientQueryMetadata md,
      final QueryStatusCount queryStatusCount
  ) {
    return new RunningQuery(
        md.getStatementString(),
        ImmutableSet.of(),
        ImmutableSet.of(),
        md.getQueryId(),
        queryStatusCount,
        md.getQueryType());
  }
}
