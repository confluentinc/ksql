package io.confluent.ksql.engine;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.BinPackedPersistentQueryMetadataImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RuntimeAssignorTest {

  @Mock
  public BinPackedPersistentQueryMetadataImpl queryMetadata;
  private final QueryId query1 = new QueryId("Test1");
  private final QueryId query2 = new QueryId("Test2");
  private final Collection<String> sourceTopics1 = Collections.singleton("test1");
  private final Collection<String> sourceTopics2 = Collections.singleton("test2");

  private String firstRuntime;
  private RuntimeAssignor runtimeAssignor;
  private static final KsqlConfig KSQL_CONFIG = new KsqlConfig(
      Collections.singletonMap(KsqlConfig.KSQL_SHARED_RUNTIMES_COUNT, 1));

  @Before
  public void setUp() {
    runtimeAssignor = new RuntimeAssignor(KSQL_CONFIG);
    firstRuntime = runtimeAssignor.getRuntimeAndMaybeAddRuntime(
        query1,
        sourceTopics1,
        KSQL_CONFIG
    );
    when(queryMetadata.getQueryApplicationId()).thenReturn(firstRuntime);
    when(queryMetadata.getQueryId()).thenReturn(query1);
    when(queryMetadata.getSourceTopicNames()).thenReturn(ImmutableSet.copyOf(new HashSet<>(sourceTopics1)));
  }

  @Test
  public void shouldCreateSandboxAndDroppingAQueryWillNotChangeReal() {
    final RuntimeAssignor sandbox = runtimeAssignor.createSandbox();
    sandbox.rebuildAssignment(Collections.singleton(queryMetadata));

    sandbox.dropQuery(queryMetadata);
    assertThat("Was changed by sandbox.", runtimeAssignor.getIdToRuntime().containsKey(query1));
    assertThat("The query was not removed.", !sandbox.getIdToRuntime().containsKey(query1));
  }

  @Test
  public void shouldCreateSandboxAndAddingAQueryWillNotChangeReal() {
    final RuntimeAssignor sandbox = runtimeAssignor.createSandbox();
    sandbox.getRuntimeAndMaybeAddRuntime(
        query2,
        sourceTopics2,
        KSQL_CONFIG
    );
    assertThat("Was changed by sandbox.", !runtimeAssignor.getIdToRuntime().containsKey(query2));
    assertThat("The query was not added.", sandbox.getIdToRuntime().containsKey(query2));
    assertThat("Added a new Runtime.", sandbox.getRuntimesToSourceTopics().size() == 1);
  }

  @Test
  public void shouldCreateSandboxAndAddingAQueryAndRuntimeWillNotChangeReal() {
    final RuntimeAssignor sandbox = runtimeAssignor.createSandbox();
    sandbox.getRuntimeAndMaybeAddRuntime(
        query2,
        sourceTopics2,
        KSQL_CONFIG
    );
    assertThat("Was changed by sandbox.", !runtimeAssignor.getIdToRuntime().containsKey(query2));
    assertThat("The query was not added.", sandbox.getIdToRuntime().containsKey(query2));
    assertThat("Was changed by sandbox.", runtimeAssignor.getRuntimesToSourceTopics().size() == 1);
  }

  @Test
  public void shouldAddingAQueryWithDifferentSourceTopicsWillNotAddARuntime() {
    runtimeAssignor.getRuntimeAndMaybeAddRuntime(
        query2,
        sourceTopics2,
        KSQL_CONFIG
    );
    assertThat("Query not added.", runtimeAssignor.getIdToRuntime().containsKey(query2));
    assertThat("Added a new Runtime.", runtimeAssignor.getRuntimesToSourceTopics().size() == 1);
  }

  @Test
  public void shouldAddingAQueryWithSameSourceTopicsWillAddARuntime() {
    runtimeAssignor.getRuntimeAndMaybeAddRuntime(
        query2,
        sourceTopics1,
        KSQL_CONFIG
    );
    assertThat("Query not added.", runtimeAssignor.getIdToRuntime().containsKey(query2));
    assertThat("Added a new Runtime.", runtimeAssignor.getRuntimesToSourceTopics().size() == 2);
  }

  @Test
  public void shouldGetSameRuntimeForSameQueryId() {
    final String runtime = runtimeAssignor.getRuntimeAndMaybeAddRuntime(
        query1,
        sourceTopics1,
        KSQL_CONFIG
    );
    assertThat(runtime, equalTo(firstRuntime));
  }

  @Test
  public void shouldNotGetSameRuntimeForDifferentQueryIdAndSameSources() {
    final String runtime = runtimeAssignor.getRuntimeAndMaybeAddRuntime(
        query2,
        sourceTopics1,
        KSQL_CONFIG
    );
    assertThat(runtime, not(equalTo(firstRuntime)));
  }

  @Test
  public void shouldDropQueryThenUseSameRuntimeForTheSameSources() {
    runtimeAssignor.dropQuery(queryMetadata);
    final String runtime = runtimeAssignor.getRuntimeAndMaybeAddRuntime(
        query1,
        sourceTopics1,
        KSQL_CONFIG
    );
    assertThat(runtime, equalTo(firstRuntime));
  }

  @Test
  public void shouldDropQueryAndCleanUpRuntime() {
    runtimeAssignor.getRuntimeAndMaybeAddRuntime(
        query2,
        sourceTopics1,
        KSQL_CONFIG
    );
    assertThat(runtimeAssignor.getRuntimesToSourceTopics().size(), equalTo(2));
    runtimeAssignor.dropQuery(queryMetadata);
    assertThat(runtimeAssignor.getRuntimesToSourceTopics().size(), equalTo(1));
  }

  @Test
  public void shouldRebuildAssignmentFromListOfQueries() {
    final RuntimeAssignor rebuilt = new RuntimeAssignor(KSQL_CONFIG);
    rebuilt.rebuildAssignment(Collections.singleton(queryMetadata));
    final String runtime = rebuilt.getRuntimeAndMaybeAddRuntime(
        query2,
        sourceTopics1,
        KSQL_CONFIG
    );
    assertThat(runtime, not(equalTo(firstRuntime)));
  }

  @Test
  public void shouldRebuildAssignmentFromLongListOfQueries() {
    //Given:
    final RuntimeAssignor rebuilt = new RuntimeAssignor(KSQL_CONFIG);
    rebuilt.rebuildAssignment(getListOfQueries());

    //When:
    final String runtime = rebuilt.getRuntimeAndMaybeAddRuntime(
        query2,
        sourceTopics1,
        KSQL_CONFIG
    );

    //Then:
    assertThat(runtime, not(equalTo(firstRuntime)));
  }

  private Collection<PersistentQueryMetadata> getListOfQueries() {
    final Collection<PersistentQueryMetadata> queries = new ArrayList<>();
    queries.add(queryMetadata);
    for (int i = 0; i < KSQL_CONFIG.getInt(KsqlConfig.KSQL_SHARED_RUNTIMES_COUNT) + 1; i++) {
      final BinPackedPersistentQueryMetadataImpl query = mock(BinPackedPersistentQueryMetadataImpl.class);
      when(query.getQueryApplicationId()).thenReturn(
          runtimeAssignor.getRuntimeAndMaybeAddRuntime(
          new QueryId(i + "_"),
              sourceTopics1,
          KSQL_CONFIG
      ));
      when(query.getQueryId()).thenReturn(query1);
      when(query.getSourceTopicNames()).thenReturn(ImmutableSet.copyOf(new HashSet<>(sourceTopics1)));
      queries.add(query);
    }
    return queries;
  }
}