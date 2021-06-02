package io.confluent.ksql.query;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.QueryEventListener;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryError.Type;
import io.confluent.ksql.query.QueryRegistryImpl.QueryExecutorFactory;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.PersistentQueryMetadataImpl;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KafkaStreams.State;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueryRegistryImplTest {
  @Mock
  private SessionConfig config;
  @Mock
  private ProcessingLogContext logContext;
  @Mock
  private QueryEventListener listener1;
  @Mock
  private QueryEventListener listener2;
  @Mock
  private QueryEventListener sandboxListener;
  @Mock
  private QueryExecutor executor;
  @Mock
  private QueryExecutorFactory executorFactory;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private MetaStore metaStore;
  @Captor
  private ArgumentCaptor<QueryMetadata.Listener> queryListenerCaptor;

  private QueryRegistryImpl registry;

  @Before
  public void setup() {
    when(executorFactory.create(any(), any(), any(), any())).thenReturn(executor);
    when(listener1.createSandbox()).thenReturn(Optional.of(sandboxListener));
    when(listener2.createSandbox()).thenReturn(Optional.empty());
    registry = new QueryRegistryImpl(ImmutableList.of(listener1, listener2), executorFactory);
  }

  @Test
  public void shouldGetAllLiveQueries() {
    // Given:
    final Set<QueryMetadata> queries = ImmutableSet.of(
        givenCreate(registry, "q1", "source", "sink", true),
        givenCreateTransient(registry, "transient1")
    );

    // When:
    final Set<QueryMetadata> listed
        = ImmutableSet.<QueryMetadata>builder().addAll(registry.getAllLiveQueries()).build();

    // Then:
    assertThat(listed, equalTo(queries));
  }

  @Test
  public void shouldGetAllLiveQueriesSandbox() {
    // Given:
    givenCreate(registry, "q1", "source", "sink", true);
    givenCreateTransient(registry, "transient1");
    final QueryRegistry sandbox = registry.createSandbox();

    // When:
    final Set<QueryMetadata> listed
        = ImmutableSet.<QueryMetadata>builder().addAll(sandbox.getAllLiveQueries()).build();

    // Then:
    final Set<String> ids = listed.stream()
        .map(q -> q.getQueryId().toString())
        .collect(Collectors.toSet());
    assertThat(ids, contains("q1", "transient1"));
  }

  @Test
  public void shouldGetPersistentQueries() {
    // Given:
    final PersistentQueryMetadata q1 = givenCreate(registry, "q1", "source", "sink1", true);
    final PersistentQueryMetadata q2 = givenCreate(registry, "q2", "source", "sink2", false);

    // When:
    final Map<QueryId, PersistentQueryMetadata> persistent = registry.getPersistentQueries();

    // Then:
    assertThat(persistent.size(), is(2));
    assertThat(persistent.get(new QueryId("q1")), is(q1));
    assertThat(persistent.get(new QueryId("q2")), is(q2));
  }

  @Test
  public void shouldGetQueriesWithSink() {
    // Given:
    givenCreate(registry, "q1", "source", "sink1", true);
    givenCreate(registry, "q2", "source", "sink1", false);
    givenCreate(registry, "q3", "source", "sink2", false);

    // When:
    final Set<QueryId> queries = registry.getQueriesWithSink(SourceName.of("sink1"));

    // Then:
    assertThat(queries, contains(new QueryId("q1"), new QueryId("q2")));
  }

  @Test
  public void shouldGetQueriesWithSinkFromSandbox() {
    // Given:
    givenCreate(registry, "q1", "source", "sink1", true);
    givenCreate(registry, "q2", "source", "sink1", false);
    givenCreate(registry, "q3", "source", "sink2", false);
    final QueryRegistry sandbox = registry.createSandbox();

    // When:
    final Set<QueryId> queries = sandbox.getQueriesWithSink(SourceName.of("sink1"));

    // Then:
    assertThat(queries, contains(new QueryId("q1"), new QueryId("q2")));
  }

  @Test
  public void shouldGetQueryThatCreatedSource() {
    // Given:
    final QueryMetadata query = givenCreate(registry, "q1", "source", "sink1", true);
    givenCreate(registry, "q2", "source", "sink1", false);
    givenCreate(registry, "q3", "source", "sink2", false);

    // When:
    final Optional<QueryMetadata> found = registry.getCreateAsQuery(SourceName.of("sink1"));

    // Then:
    assertThat(found.get(), is(query));
  }

  @Test
  public void shouldGetQueryThatCreatedSourceOnSandbox() {
    // Given:
    givenCreate(registry, "q1", "source", "sink1", true);
    givenCreate(registry, "q2", "source", "sink1", false);
    givenCreate(registry, "q3", "source", "sink2", false);
    final QueryRegistry sandbox = registry.createSandbox();

    // When:
    final Optional<QueryMetadata> found = sandbox.getCreateAsQuery(SourceName.of("sink1"));

    // Then:
    assertThat(found.get().getQueryId().toString(), equalTo("q1"));
  }

  @Test
  public void shouldGetQueriesInsertingIntoOrReadingFromSource() {
    givenCreate(registry, "q1", "source", "sink1", true);
    givenCreate(registry, "q2", "source", "sink1", false);
    givenCreate(registry, "q3", "source", "sink1", false);
    givenCreate(registry, "q4", "sink1", "sink2", false);

    // When:
    final Set<QueryId> queries = registry.getInsertQueries(SourceName.of("sink1"), (n, q) -> true);

    // Then:
    assertThat(queries, contains(new QueryId("q2"), new QueryId("q3"), new QueryId("q4")));
  }

  @Test
  public void shouldRemoveQueryFromCreateAsQueriesWhenTerminatingCreateAsQuery() {
    // Given:
    givenCreate(registry, "q1", "source", "sink1", true);
    givenCreate(registry, "i1", "source", "sink1", false);
    final QueryRegistry sandbox = registry.createSandbox();
    final QueryMetadata q1 = sandbox.getPersistentQuery(new QueryId("q1")).get();
    final QueryMetadata i1 = sandbox.getPersistentQuery(new QueryId("i1")).get();

    // When:
    q1.close();
    final Optional<QueryMetadata> createAsQueries =
        sandbox.getCreateAsQuery(SourceName.of("sink1"));
    final Set<QueryId> insertQueries =
        sandbox.getInsertQueries(SourceName.of("sink1"), (n, q) -> true);

    // Then:
    assertThat(createAsQueries, equalTo(Optional.empty()));
    assertThat(insertQueries, contains(i1.getQueryId()));
  }

  @Test
  public void shouldRemoveQueryFromInsertQueriesWhenTerminatingInsertQuery() {
    // Given:
    givenCreate(registry, "q1", "source", "sink1", true);
    givenCreate(registry, "i1", "source", "sink1", false);
    final QueryRegistry sandbox = registry.createSandbox();
    final QueryMetadata q1 = sandbox.getPersistentQuery(new QueryId("q1")).get();
    final QueryMetadata i1 = sandbox.getPersistentQuery(new QueryId("i1")).get();

    // When:
    i1.close();
    final QueryMetadata createAsQueries = sandbox.getCreateAsQuery(SourceName.of("sink1")).get();
    final Set<QueryId> insertQueries =
        sandbox.getInsertQueries(SourceName.of("sink1"), (n, q) -> true);

    // Then:
    assertThat(createAsQueries, equalTo(q1));
    assertThat(insertQueries, empty());
  }

  @Test
  public void shouldCopyInsertsOnSandbox() {
    // Given:
    givenCreate(registry, "q1", "source", "sink1", true);
    givenCreate(registry, "q2", "source", "sink1", false);
    givenCreate(registry, "q3", "source", "sink1", false);
    final QueryRegistry sandbox = registry.createSandbox();

    // When:
    final Set<QueryId> queries = sandbox.getInsertQueries(SourceName.of("sink1"), (n, q) -> true);

    // Then:
    assertThat(queries, contains(new QueryId("q2"), new QueryId("q3")));
  }

  @Test
  public void shouldCallListenerOnCreate() {
    // Given/When:
    final QueryMetadata query = givenCreate(registry, "q1", "source", "sink1", true);

    // Then:
    verify(listener1).onCreate(serviceContext, metaStore, query);
    verify(listener2).onCreate(serviceContext, metaStore, query);
  }

  @Test
  public void shouldCallListenerOnClose() {
    // Given:
    final QueryMetadata.Listener queryListener = givenCreateGetListener(registry, "foo");
    final QueryMetadata query = registry.getPersistentQuery(new QueryId("foo")).get();

    // When:
    queryListener.onClose(query);

    // Then:
    verify(listener1).onClose(query);
    verify(listener2).onClose(query);
  }

  @Test
  public void shouldCallListenerOnStateChange() {
    // Given:
    final QueryMetadata.Listener queryListener = givenCreateGetListener(registry, "foo");
    final QueryMetadata query = registry.getPersistentQuery(new QueryId("foo")).get();

    // When:
    queryListener.onStateChange(query, State.CREATED, State.RUNNING);

    // Then:
    verify(listener1).onStateChange(query, State.CREATED, State.RUNNING);
    verify(listener2).onStateChange(query, State.CREATED, State.RUNNING);
  }

  @Test
  public void shouldCallListenerOnError() {
    // Given:
    final QueryMetadata.Listener queryListener = givenCreateGetListener(registry, "foo");
    final QueryMetadata query = registry.getPersistentQuery(new QueryId("foo")).get();
    final QueryError error = new QueryError(123L, "error", Type.USER);

    // When:
    queryListener.onError(query, error);

    // Then:
    verify(listener1).onError(query, error);
    verify(listener2).onError(query, error);
  }

  @Test
  public void shouldCallSandboxOnCreate() {
    // Given:
    final QueryRegistry sandbox = registry.createSandbox();

    // When:
    final PersistentQueryMetadata q = givenCreate(sandbox, "q1", "source", "sink1", true);

    // Then:
    verify(sandboxListener).onCreate(serviceContext, metaStore, q);
    verify(listener1, times(0)).onCreate(any(), any(), any());
    verify(listener2, times(0)).onCreate(any(), any(), any());
  }

  @Test
  public void shouldCallSandboxOnCloseOld() {
    // Given:
    givenCreate(registry, "q1", "source", "sink1", true);
    final QueryRegistry sandbox = registry.createSandbox();
    final QueryMetadata sandboxQuery = sandbox.getPersistentQuery(new QueryId("q1")).get();

    // When:
    sandboxQuery.close();

    // Then:
    verify(sandboxListener).onClose(sandboxQuery);
    verify(sandboxListener).onDeregister(sandboxQuery);
    verify(listener1, times(0)).onClose(any());
    verify(listener1, times(0)).onDeregister(any());
    verify(listener2, times(0)).onClose(any());
    verify(listener2, times(0)).onDeregister(any());
  }

  @Test
  public void shouldCallSandboxOnClose() {
    // Given:
    final QueryRegistry sandbox = registry.createSandbox();
    final QueryMetadata.Listener queryListener = givenCreateGetListener(sandbox, "foo");
    final QueryMetadata query = sandbox.getPersistentQuery(new QueryId("foo")).get();

    // When:
    queryListener.onClose(query);

    // Then:
    verify(sandboxListener).onClose(query);
    verify(listener1, times(0)).onClose(any());
    verify(listener2, times(0)).onClose(any());
  }

  private QueryMetadata.Listener givenCreateGetListener(
      final QueryRegistry registry,
      final String id
  ) {
    givenCreate(registry, id, "source", "sink1", true);
    verify(executor).buildPersistentQuery(
        any(), any(), any(), any(), any(), any(), any(), queryListenerCaptor.capture(), any());
    return queryListenerCaptor.getValue();
  }

  private PersistentQueryMetadata givenCreate(
      final QueryRegistry registry,
      final String id,
      final String source,
      final String sink,
      boolean createAs
  ) {
    final QueryId queryId = new QueryId(id);
    final PersistentQueryMetadata query = mock(PersistentQueryMetadataImpl.class);
    final DataSource sinkSource = mock(DataSource.class);
    when(sinkSource.getName()).thenReturn(SourceName.of(sink));
    when(query.getQueryId()).thenReturn(queryId);
    when(query.getSinkName()).thenReturn(SourceName.of(sink));
    when(query.getSink()).thenReturn(sinkSource);
    when(query.getSourceNames()).thenReturn(ImmutableSet.of(SourceName.of(source)));
    when(query.getPersistentQueryType()).thenReturn(createAs
        ? KsqlConstants.PersistentQueryType.CREATE_AS
        : KsqlConstants.PersistentQueryType.INSERT);
    when(executor.buildPersistentQuery(
        any(), any(), any(), any(), any(), any(), any(), any(), any())
    ).thenReturn(query);
    registry.createOrReplacePersistentQuery(
        config,
        serviceContext,
        logContext,
        metaStore,
        "sql",
        queryId,
        sinkSource,
        ImmutableSet.of(SourceName.of(source)),
        mock(ExecutionStep.class),
        "plan-summary",
        createAs
    );
    return query;
  }

  private TransientQueryMetadata givenCreateTransient(
      final QueryRegistry registry,
      final String id
  ) {
    final QueryId queryId = new QueryId(id);
    final TransientQueryMetadata query = mock(TransientQueryMetadata.class);
    when(query.getQueryId()).thenReturn(queryId);
    when(executor.buildTransientQuery(
        any(), any(), any(), any(), any(), any(), any(), any(), anyBoolean(), any())
    ).thenReturn(query);
    registry.createTransientQuery(
        config,
        serviceContext,
        logContext,
        metaStore,
        "sql",
        queryId,
        ImmutableSet.of(SourceName.of("some-source")),
        mock(ExecutionStep.class),
        "plan-summary",
        mock(LogicalSchema.class),
        OptionalInt.of(123),
        Optional.empty(),
        false
    );
    return query;
  }
}