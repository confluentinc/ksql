package io.confluent.ksql.query;

import static io.confluent.ksql.util.KsqlConstants.PersistentQueryType.CREATE_AS;
import static io.confluent.ksql.util.KsqlConstants.PersistentQueryType.CREATE_SOURCE;
import static io.confluent.ksql.util.KsqlConstants.PersistentQueryType.INSERT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.engine.QueryEventListener;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryError.Type;
import io.confluent.ksql.query.QueryRegistryImpl.QueryBuilderFactory;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.BinPackedPersistentQueryMetadataImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.PersistentQueryMetadataImpl;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.QueryMetadataImpl;
import io.confluent.ksql.util.SharedKafkaStreamsRuntime;
import io.confluent.ksql.util.SharedKafkaStreamsRuntimeImpl;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams.State;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

@RunWith(Parameterized.class)
public class QueryRegistryImplTest {
  @Mock
  private SessionConfig config;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private ProcessingLogContext logContext;
  @Mock
  private QueryEventListener listener1;
  @Mock
  private QueryEventListener listener2;
  @Mock
  private QueryEventListener sandboxListener;
  @Mock
  private QueryBuilder queryBuilder;
  @Mock
  private QueryBuilderFactory executorFactory;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private MetaStore metaStore;
  @Captor
  private ArgumentCaptor<QueryMetadata.Listener> queryListenerCaptor;
  @SuppressWarnings("Unused")
  @Rule
  public MockitoRule rule = MockitoJUnit.rule();

  private QueryRegistryImpl registry;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Boolean> data() {
    return Arrays.asList(
        true, false
    );
  }

  @Parameterized.Parameter
  public boolean sharedRuntimes;

  @Before
  public void setup() {
    rule.strictness(Strictness.WARN);
    when(executorFactory.create(any(), any(), any(), any(), any(), anyBoolean())).thenReturn(queryBuilder);
    when(listener1.createSandbox()).thenReturn(Optional.of(sandboxListener));
    when(listener2.createSandbox()).thenReturn(Optional.empty());
    registry = new QueryRegistryImpl(ImmutableList.of(listener1, listener2), executorFactory, new MetricCollectors());
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED)).thenReturn(sharedRuntimes);
  }

  @Test
  public void shouldGetAllLiveQueries() {
    // Given:
    final Set<QueryMetadata> queries = ImmutableSet.of(
        givenCreate(registry, "q1", "source1", Optional.of("sink"), CREATE_AS),
        givenCreate(registry, "q2", "source2", Optional.empty(), CREATE_SOURCE),
        givenCreateTransient(registry, "transient1")
    );

    // When:
    final Set<QueryMetadata> listed
        = ImmutableSet.<QueryMetadata>builder().addAll(registry.getAllLiveQueries()).build();

    // Then:
    assertThat(listed, equalTo(queries));
  }

  @Test
  public void shouldNotIncludeStreamPullInLiveQueries() {
    // Given:
    final Set<QueryMetadata> queries = ImmutableSet.of(
        givenCreate(registry, "q1", "source1", Optional.of("sink"), CREATE_AS),
        givenCreate(registry, "q2", "source2", Optional.empty(), CREATE_SOURCE),
        givenCreateTransient(registry, "transient1")
    );
    givenStreamPull(registry, "streamPull1");

    // When:
    final Set<QueryMetadata> listed
        = ImmutableSet.<QueryMetadata>builder().addAll(registry.getAllLiveQueries()).build();

    // Then:
    assertThat(listed, equalTo(queries));
  }

  @Test
  public void shouldNotUseGlobalConfigIfQueryPlanOverrides() {
    // Given:
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED)).thenReturn(!sharedRuntimes);
    QueryMetadata query = givenCreate(registry, "q1", "source", Optional.of("sink"), CREATE_AS);

    verify(listener1).onCreate(serviceContext, metaStore, query);
  }

  @Test
  public void shouldGetAllLiveQueriesSandbox() {
    // Given:
    givenCreate(registry, "q1", "source", Optional.of("sink"), CREATE_AS);
    givenCreate(registry, "q2", "source", Optional.empty(), CREATE_SOURCE);
    givenCreateTransient(registry, "transient1");
    final QueryRegistry sandbox = registry.createSandbox();

    // When:
    final Set<QueryMetadata> listed
        = ImmutableSet.<QueryMetadata>builder().addAll(sandbox.getAllLiveQueries()).build();

    // Then:
    final Set<String> ids = listed.stream()
        .map(q -> q.getQueryId().toString())
        .collect(Collectors.toSet());
    assertThat(ids, contains("q1", "q2", "transient1"));
  }

  @Test
  public void shouldGetPersistentQueries() {
    // Given:
    final PersistentQueryMetadata q1 = givenCreate(registry, "q1", "source",
        Optional.of("sink1"), CREATE_AS);
    final PersistentQueryMetadata q2 = givenCreate(registry, "q2", "source",
        Optional.of("sink2"), INSERT);
    final PersistentQueryMetadata q3 = givenCreate(registry, "q3", "source",
        Optional.empty(), CREATE_SOURCE);

    // When:
    final Map<QueryId, PersistentQueryMetadata> persistent = registry.getPersistentQueries();

    // Then:
    assertThat(persistent.size(), is(3));
    assertThat(persistent.get(new QueryId("q1")), is(q1));
    assertThat(persistent.get(new QueryId("q2")), is(q2));
    assertThat(persistent.get(new QueryId("q3")), is(q3));
  }

  @Test
  public void shouldGetQuery() {
    // Given:
    final TransientQueryMetadata q1 = givenCreateTransient(registry, "transient1");
    final PersistentQueryMetadata q2 = givenCreate(registry, "q1", "source",
        Optional.of("sink1"), CREATE_AS);
    final PersistentQueryMetadata q3 = givenCreate(registry, "q2", "source",
        Optional.empty(), CREATE_SOURCE);

    // When:
    final QueryMetadata queryMetadata1 = registry.getQuery(q1.getQueryId()).get();
    final QueryMetadata queryMetadata2 = registry.getQuery(q2.getQueryId()).get();
    final QueryMetadata queryMetadata3 = registry.getQuery(q3.getQueryId()).get();

    // Then:
    assertThat(queryMetadata1, is(q1));
    assertThat(queryMetadata2, is(q2));
    assertThat(queryMetadata3, is(q3));
  }

  @Test
  public void shouldGetQueriesWithSink() {
    // Given:
    givenCreate(registry, "q1", "source", Optional.of("sink1"), CREATE_AS);
    givenCreate(registry, "q2", "source", Optional.of("sink1"), INSERT);
    givenCreate(registry, "q3", "source", Optional.of("sink2"), INSERT);
    givenCreate(registry, "q4", "source", Optional.empty(), CREATE_SOURCE);

    // When:
    final Set<QueryId> queries = registry.getQueriesWithSink(SourceName.of("sink1"));

    // Then:
    assertThat(queries, contains(new QueryId("q1"), new QueryId("q2")));
  }

  @Test
  public void shouldGetQueriesWithSinkFromSandbox() {
    // Given:
    givenCreate(registry, "q1", "source", Optional.of("sink1"), CREATE_AS);
    givenCreate(registry, "q2", "source", Optional.of("sink1"), INSERT);
    givenCreate(registry, "q3", "source", Optional.of("sink2"), INSERT);
    givenCreate(registry, "q4", "source", Optional.empty(), CREATE_SOURCE);
    final QueryRegistry sandbox = registry.createSandbox();

    // When:
    final Set<QueryId> queries = sandbox.getQueriesWithSink(SourceName.of("sink1"));

    // Then:
    assertThat(queries, contains(new QueryId("q1"), new QueryId("q2")));
  }

  @Test
  public void shouldGetQueryThatCreatedSource() {
    // Given:
    final QueryMetadata query = givenCreate(registry, "q1", "source",
        Optional.of("sink1"), CREATE_AS);
    givenCreate(registry, "q2", "source", Optional.of("sink1"), INSERT);
    givenCreate(registry, "q3", "source", Optional.of("sink2"), INSERT);

    // When:
    final Optional<QueryMetadata> found = registry.getCreateAsQuery(SourceName.of("sink1"));

    // Then:
    assertThat(found.get(), is(query));
  }

  @Test
  public void shouldOnlyAllowServerLevelConfigsForDedicatedRuntimesSandbox() {
    // Given:
    when(config.getOverrides()).thenReturn(ImmutableMap.of("commit.interval.ms", 9));
    if (sharedRuntimes) {
      final Exception e = assertThrows(IllegalArgumentException.class,
          () -> givenCreate(registry.createSandbox(), "q1", "source",
          Optional.of("sink1"), CREATE_AS));
      assertThat(e.getMessage(), containsString("commit.interval.ms"));
    }
    givenCreate(registry, "q1", "source", Optional.of("sink1"), CREATE_AS);
  }

  @Test
  public void shouldGetQueryThatCreatedSourceOnSandbox() {
    // Given:
    givenCreate(registry, "q1", "source", Optional.of("sink1"), CREATE_AS);
    givenCreate(registry, "q2", "source", Optional.of("sink1"), INSERT);
    givenCreate(registry, "q3", "source", Optional.of("sink2"), INSERT);
    final QueryRegistry sandbox = registry.createSandbox();

    // When:
    final Optional<QueryMetadata> found = sandbox.getCreateAsQuery(SourceName.of("sink1"));

    // Then:
    assertThat(found.get().getQueryId().toString(), equalTo("q1"));
  }

  @Test
  public void shouldGetQueriesInsertingIntoOrReadingFromSource() {
    givenCreate(registry, "q1", "source", Optional.of("sink1"), CREATE_AS);
    givenCreate(registry, "q2", "source", Optional.of("sink1"), INSERT);
    givenCreate(registry, "q3", "source", Optional.of("sink1"), INSERT);
    givenCreate(registry, "q4", "sink1", Optional.of("sink2"), INSERT);

    // When:
    final Set<QueryId> queries = registry.getInsertQueries(SourceName.of("sink1"), (n, q) -> true);

    // Then:
    assertThat(queries, contains(new QueryId("q2"), new QueryId("q3"), new QueryId("q4")));
  }

  @Test
  public void shouldRemoveQueryFromCreateAsQueriesWhenTerminatingCreateAsQuery() {
    // Given:
    givenCreate(registry, "q1", "source", Optional.of("sink1"), CREATE_AS);
    givenCreate(registry, "q2", "source", Optional.empty(), CREATE_SOURCE);
    givenCreate(registry, "i1", "source", Optional.of("sink1"), INSERT);
    final QueryRegistry sandbox = registry.createSandbox();
    final QueryMetadata q1 = sandbox.getPersistentQuery(new QueryId("q1")).get();
    final QueryMetadata q2 = sandbox.getPersistentQuery(new QueryId("q2")).get();
    final QueryMetadata i1 = sandbox.getPersistentQuery(new QueryId("i1")).get();

    // When:
    q1.close();
    final Optional<QueryMetadata> createAsQueries =
        sandbox.getCreateAsQuery(SourceName.of("sink1"));
    final Optional<QueryMetadata> createSourceQueries =
        sandbox.getCreateAsQuery(SourceName.of("source"));
    final Set<QueryId> insertQueries =
        sandbox.getInsertQueries(SourceName.of("sink1"), (n, q) -> true);

    // Then:
    assertThat(createAsQueries, equalTo(Optional.empty()));
    assertThat(createSourceQueries, equalTo(Optional.of(q2)));
    assertThat(insertQueries, contains(i1.getQueryId()));
  }

  @Test
  public void shouldRemoveQueryFromInsertQueriesWhenTerminatingInsertQuery() {
    // Given:
    givenCreate(registry, "q1", "source", Optional.of("sink1"), CREATE_AS);
    givenCreate(registry, "q2", "source", Optional.empty(), CREATE_SOURCE);
    givenCreate(registry, "i1", "source", Optional.of("sink1"), INSERT);
    final QueryRegistry sandbox = registry.createSandbox();
    final QueryMetadata q1 = sandbox.getPersistentQuery(new QueryId("q1")).get();
    final QueryMetadata q2 = sandbox.getPersistentQuery(new QueryId("q2")).get();
    final QueryMetadata i1 = sandbox.getPersistentQuery(new QueryId("i1")).get();

    // When:
    i1.close();
    final QueryMetadata createAsQueries = sandbox.getCreateAsQuery(SourceName.of("sink1")).get();
    final QueryMetadata createSourceQueries =
        sandbox.getCreateAsQuery(SourceName.of("source")).get();
    final Set<QueryId> insertQueries =
        sandbox.getInsertQueries(SourceName.of("sink1"), (n, q) -> true);

    // Then:
    assertThat(createAsQueries, equalTo(q1));
    assertThat(createSourceQueries, equalTo(q2));
    assertThat(insertQueries, empty());
  }

  @Test
  public void shouldRemoveQueryFromCreateAsQueriesWhenTerminatingCreateSourceQuery() {
    // Given:
    givenCreate(registry, "q1", "source", Optional.of("sink1"), CREATE_AS);
    givenCreate(registry, "q2", "source", Optional.empty(), CREATE_SOURCE);
    givenCreate(registry, "i1", "source", Optional.of("sink1"), INSERT);
    final QueryRegistry sandbox = registry.createSandbox();
    final QueryMetadata q1 = sandbox.getPersistentQuery(new QueryId("q1")).get();
    final QueryMetadata q2 = sandbox.getPersistentQuery(new QueryId("q2")).get();
    final QueryMetadata i1 = sandbox.getPersistentQuery(new QueryId("i1")).get();

    // When:
    q2.close();
    final QueryMetadata createAsQueries = sandbox.getCreateAsQuery(SourceName.of("sink1")).get();
    final Optional<QueryMetadata> createSourceQueries =
        sandbox.getCreateAsQuery(SourceName.of("source"));
    final Set<QueryId> insertQueries =
        sandbox.getInsertQueries(SourceName.of("sink1"), (n, q) -> true);

    // Then:
    assertThat(createAsQueries, equalTo(q1));
    assertThat(createSourceQueries, equalTo(Optional.empty()));
    assertThat(insertQueries, contains(i1.getQueryId()));
  }

  @Test
  public void shouldCopyInsertsOnSandbox() {
    // Given:
    givenCreate(registry, "q1", "source", Optional.of("sink1"), CREATE_AS);
    givenCreate(registry, "q2", "source", Optional.of("sink1"), INSERT);
    givenCreate(registry, "q3", "source", Optional.of("sink1"), INSERT);
    final QueryRegistry sandbox = registry.createSandbox();

    // When:
    final Set<QueryId> queries = sandbox.getInsertQueries(SourceName.of("sink1"), (n, q) -> true);

    // Then:
    assertThat(queries, contains(new QueryId("q2"), new QueryId("q3")));
  }

  @Test
  public void shouldCallListenerOnCreate() {
    // Given/When:
    final QueryMetadata query = givenCreate(registry, "q1", "source",
        Optional.of("sink1"), CREATE_AS);

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
  public void shouldRegisterQuery() {
    //When:
    final PersistentQueryMetadata q = givenCreate(registry, "q1", "source",
        Optional.of("sink1"), CREATE_AS);

    //Then:
    if (sharedRuntimes) {
      verify(q).register();
    }
    verify(q, never()).start();
  }

  @Test
  public void shouldCallSandboxOnCreate() {
    // Given:
    final QueryRegistry sandbox = registry.createSandbox();

    // When:
    final PersistentQueryMetadata q = givenCreate(sandbox, "q1", "source",
        Optional.of("sink1"), CREATE_AS);

    // Then:
    verify(sandboxListener).onCreate(serviceContext, metaStore, q);
    verify(listener1, times(0)).onCreate(any(), any(), any());
    verify(listener2, times(0)).onCreate(any(), any(), any());
  }

  @Test
  public void shouldCallSandboxOnCloseOld() {
    // Given:
    givenCreate(registry, "q1", "source", Optional.of("sink1"), CREATE_AS);
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

  @Test
  public void shouldReplaceQueryfromOldRuntimeUsingOldRuntime() {
    //Given:
    sharedRuntimes = false;
    QueryMetadata query = givenCreate(registry, "q1", "source",
        Optional.of("sink1"), CREATE_AS);
    assertThat("does not use old runtime", query instanceof PersistentQueryMetadataImpl);
    //When:
    sharedRuntimes = true;
    query = givenCreate(registry, "q1", "source",
        Optional.of("sink1"), CREATE_AS);
    //Expect:
    assertThat("does not use old runtime", query instanceof PersistentQueryMetadataImpl);
    when(ksqlConfig.getBoolean(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED)).thenReturn(sharedRuntimes);
    query = givenCreate(registry, "q2", "source",
        Optional.of("sink1"), CREATE_AS);
    assertThat("does not use old runtime", query instanceof BinPackedPersistentQueryMetadataImpl);

  }

  private QueryMetadata.Listener givenCreateGetListener(
      final QueryRegistry registry,
      final String id
  ) {
    givenCreate(registry, id, "source", Optional.of("sink1"), CREATE_AS);
    if (!sharedRuntimes) {
      verify(queryBuilder).buildPersistentQueryInDedicatedRuntime(
          any(), any(), any(), any(), any(), any(), any(), any(), queryListenerCaptor.capture(), any(), any(), any());
    } else {
      verify(queryBuilder).buildPersistentQueryInSharedRuntime(
          any(), any(), any(), any(), any(), any(), any(), any(), queryListenerCaptor.capture(), any(), any(), any());
    }
    return queryListenerCaptor.getValue();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private PersistentQueryMetadata givenCreate(
      final QueryRegistry registry,
      final String id,
      final String source,
      final Optional<String> sink,
      KsqlConstants.PersistentQueryType persistentQueryType
  ) {
    final QueryId queryId = new QueryId(id);
    final PersistentQueryMetadata query = mock(PersistentQueryMetadataImpl.class);
    final PersistentQueryMetadata newQuery = mock(BinPackedPersistentQueryMetadataImpl.class);
    final DataSource sinkSource = mock(DataSource.class);
    final ExecutionStep physicalPlan = mock(ExecutionStep.class);

    sink.ifPresent(s -> {
      when(sinkSource.getName()).thenReturn(SourceName.of(s));
      when(query.getSinkName()).thenReturn(Optional.of(SourceName.of(s)));
      when(newQuery.getSinkName()).thenReturn(Optional.of(SourceName.of(s)));
    });

    when(newQuery.getOverriddenProperties()).thenReturn(new HashMap<>());
    when(newQuery.getQueryId()).thenReturn(queryId);
    when(newQuery.getSink()).thenReturn(Optional.of(sinkSource));
    when(newQuery.getSourceNames()).thenReturn(ImmutableSet.of(SourceName.of(source)));
    when(newQuery.getPersistentQueryType()).thenReturn(persistentQueryType);
    when(newQuery.getPhysicalPlan()).thenReturn(physicalPlan);
    final SharedKafkaStreamsRuntime runtime = mock(SharedKafkaStreamsRuntimeImpl.class);
    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getName()).thenReturn(SourceName.of(source));
    try {
      Field sharedRuntime = BinPackedPersistentQueryMetadataImpl.class.getDeclaredField("sharedKafkaStreamsRuntime");
      sharedRuntime.setAccessible(true);
      sharedRuntime.set(newQuery, runtime);
      Field sourc = BinPackedPersistentQueryMetadataImpl.class.getDeclaredField("sources");
      sourc.setAccessible(true);
      sourc.set(newQuery, ImmutableSet.of(dataSource));
    } catch (final NoSuchFieldException | IllegalAccessException e) {
      e.printStackTrace();
    }


    when(runtime.getNewQueryErrorQueue()).thenReturn(mock(QueryMetadataImpl.TimeBoundedQueue.class));

    when(query.getQueryId()).thenReturn(queryId);
    when(query.getSink()).thenReturn(Optional.of(sinkSource));
    when(query.getSourceNames()).thenReturn(ImmutableSet.of(SourceName.of(source)));
    when(query.getPersistentQueryType()).thenReturn(persistentQueryType);
    when(query.getPhysicalPlan()).thenReturn(physicalPlan);
    when(queryBuilder.buildPersistentQueryInSharedRuntime(
        any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any())
    ).thenReturn(newQuery);
    when(queryBuilder.buildPersistentQueryInDedicatedRuntime(
        any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any())
    ).thenReturn(query);

    when(config.getConfig(true)).thenReturn(ksqlConfig);
    return registry.createOrReplacePersistentQuery(
        config,
        serviceContext,
        logContext,
        metaStore,
        "sql",
        queryId,
        Optional.of(sinkSource),
        ImmutableSet.of(dataSource),
        mock(ExecutionStep.class),
        "plan-summary",
        persistentQueryType,
        sharedRuntimes ? Optional.of("applicationId") : Optional.empty()
    );
  }

  private TransientQueryMetadata givenCreateTransient(
      final QueryRegistry registry,
      final String id
  ) {
    final QueryId queryId = new QueryId(id);
    final TransientQueryMetadata query = mock(TransientQueryMetadata.class);
    when(query.getQueryId()).thenReturn(queryId);
    when(queryBuilder.buildTransientQuery(
        any(), any(), any(), any(), any(), any(), any(), any(), anyBoolean(), any(), any(), any(), any())
    ).thenReturn(query);
    when(query.isInitialized()).thenReturn(true);
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

  private void givenStreamPull(
      final QueryRegistry registry,
      final String id
  ) {
    final QueryId queryId = new QueryId(id);
    final TransientQueryMetadata query = mock(TransientQueryMetadata.class);
    when(query.getQueryId()).thenReturn(queryId);
    when(queryBuilder.buildTransientQuery(
        any(), any(), any(), any(), any(), any(), any(), any(), anyBoolean(), any(), any(), any(), any())
    ).thenReturn(query);
    when(query.isInitialized()).thenReturn(true);
    registry.createStreamPullQuery(
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
        false,
        ImmutableMap.<TopicPartition, Long> builder().build()
    );
  }
}