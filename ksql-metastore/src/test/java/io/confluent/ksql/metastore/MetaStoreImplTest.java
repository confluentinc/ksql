/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.metastore;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlReferentialIntegrityException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
@RunWith(MockitoJUnitRunner.class)
public class MetaStoreImplTest {

  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private KsqlTopic topic;
  @Mock
  private DataSource<?> dataSource;
  @Mock
  private DataSource<?> dataSource1;
  private MetaStoreImpl metaStore;
  private ExecutorService executor;

  @Before
  public void setUp() {
    metaStore = new MetaStoreImpl(functionRegistry);

    when(topic.getKsqlTopicName()).thenReturn("some registered topic");
    when(dataSource.getName()).thenReturn("some source");
    when(dataSource1.getName()).thenReturn("some other source");

    executor = Executors.newSingleThreadExecutor();
  }

  @After
  public void tearDown() throws Exception {
    executor.shutdownNow();
    executor.awaitTermination(1, TimeUnit.MINUTES);
  }

  @Test
  public void shouldDeepCopyTopicsOnCopy() {
    // Given:
    metaStore.putTopic(topic);

    // When:
    final MetaStore copy = metaStore.copy();
    metaStore.deleteTopic(topic.getKsqlTopicName());

    // Then:
    assertThat(copy.getAllKsqlTopics().keySet(), contains(topic.getKsqlTopicName()));
    assertThat(metaStore.getAllKsqlTopics().keySet(), is(empty()));
  }

  @Test
  public void shouldDeepCopySourcesOnCopy() {
    // Given:
    metaStore.putSource(dataSource);

    // When:
    final MetaStore copy = metaStore.copy();
    metaStore.deleteSource(dataSource.getName());

    // Then:
    assertThat(copy.getAllDataSources().keySet(), contains(dataSource.getName()));
    assertThat(metaStore.getAllDataSources().keySet(), is(empty()));
  }

  @Test
  public void shouldDeepCopySourceReferentialIntegrityDataOnCopy() {
    // Given:
    metaStore.putSource(dataSource);

    metaStore.updateForPersistentQuery(
        "source query",
        ImmutableSet.of(dataSource.getName()),
        ImmutableSet.of());

    metaStore.updateForPersistentQuery(
        "sink query",
        ImmutableSet.of(),
        ImmutableSet.of(dataSource.getName()));

    // When:
    final MetaStore copy = metaStore.copy();
    metaStore.removePersistentQuery("source query");
    metaStore.removePersistentQuery("sink query");

    // Then:
    assertThat(copy.getQueriesWithSource(dataSource.getName()), contains("source query"));
    assertThat(metaStore.getQueriesWithSource(dataSource.getName()), is(empty()));
    assertThat(copy.getQueriesWithSink(dataSource.getName()), contains("sink query"));
    assertThat(metaStore.getQueriesWithSink(dataSource.getName()), is(empty()));
  }

  @Test
  public void shouldNotAllowModificationViaGetAllDataSources() {
    // Given:
    metaStore.putSource(dataSource);

    final Map<String, DataSource<?>> dataSources = metaStore
        .getAllDataSources();

    // When
    dataSources.keySet().clear();

    // Then:
    assertThat(metaStore.getAllDataSources().keySet(), contains(dataSource.getName()));
  }

  @Test
  public void shouldNotAllowModificationViaGetAllKsqlTopics() {
    // Given:
    metaStore.putTopic(topic);

    final Map<String, KsqlTopic> topics = metaStore.getAllKsqlTopics();

    // When
    assertThrows(
        UnsupportedOperationException.class,
        () -> topics.keySet().clear()
    );
  }

  @Test
  public void shouldThrowOnDuplicateTopic() {
    // Given:
    metaStore.putTopic(topic);

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> metaStore.putTopic(topic)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Another topic with the same name already exists"));
  }

  @Test
  public void shouldThrowOnRemoveUnknownTopic() {
    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> metaStore.deleteTopic("bob")
    );

    // Then:
    assertThat(e.getMessage(), containsString("No topic with name bob was registered"));
  }

  @Test
  public void shouldThrowOnDuplicateSource() {
    // Given:
    metaStore.putSource(dataSource);

    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> metaStore.putSource(dataSource)
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("Another data source with the same name already exists"));
  }

  @Test
  public void shouldThrowOnRemoveUnknownSource() {
    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> metaStore.deleteSource("bob")
    );

    // Then:
    assertThat(e.getMessage(), containsString("No data source with name bob exists"));
  }

  @Test
  public void shouldThrowOnDropSourceIfUsedAsSourceOfQueries() {
    // Given:
    metaStore.putSource(dataSource);
    metaStore.updateForPersistentQuery(
        "source query",
        ImmutableSet.of(dataSource.getName()),
        ImmutableSet.of());

    // When:
    final KsqlReferentialIntegrityException e = assertThrows(
        KsqlReferentialIntegrityException.class,
        () -> metaStore.deleteSource(dataSource.getName())
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("The following queries read from this source: [source query]"));
  }

  @Test
  public void shouldThrowOnUpdateForPersistentQueryOnUnknownSource() {
    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> metaStore.updateForPersistentQuery(
            "source query",
            ImmutableSet.of("unknown"),
            ImmutableSet.of())
    );

    // Then:
    assertThat(e.getMessage(), containsString("Unknown source: unknown"));
  }

  @Test
  public void shouldThrowOnUpdateForPersistentQueryOnUnknownSink() {
    // When:
    final KsqlException e = assertThrows(
        KsqlException.class,
        () -> metaStore.updateForPersistentQuery(
            "sink query",
            ImmutableSet.of(),
            ImmutableSet.of("unknown"))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Unknown source: unknown"));
  }

  @Test
  public void shouldThrowOnDropSourceIfUsedAsSinkOfQueries() {
    // Given:
    metaStore.putSource(dataSource);
    metaStore.updateForPersistentQuery(
        "sink query",
        ImmutableSet.of(),
        ImmutableSet.of(dataSource.getName()));

    // When:
    final KsqlReferentialIntegrityException e = assertThrows(
        KsqlReferentialIntegrityException.class,
        () -> metaStore.deleteSource(dataSource.getName())
    );

    // Then:
    assertThat(e.getMessage(),
        containsString("The following queries write into this source: [sink query]"));
  }

  @Test
  public void shouldFailToUpdateForPersistentQueryAtomicallyForUnknownSource() {
    // When:
    try {
      metaStore.updateForPersistentQuery(
          "some query",
          ImmutableSet.of(dataSource.getName(), "unknown"),
          ImmutableSet.of(dataSource.getName()));
    } catch (final KsqlException e) {
      // Expected
    }

    // Then:
    assertThat(metaStore.getQueriesWithSource(dataSource.getName()), is(empty()));
    assertThat(metaStore.getQueriesWithSink(dataSource.getName()), is(empty()));
  }

  @Test
  public void shouldFailToUpdateForPersistentQueryAtomicallyForUnknownSink() {
    // When:
    try {
      metaStore.updateForPersistentQuery(
          "some query",
          ImmutableSet.of(dataSource.getName()),
          ImmutableSet.of(dataSource.getName(), "unknown"));
    } catch (final KsqlException e) {
      // Expected
    }

    // Then:
    assertThat(metaStore.getQueriesWithSource(dataSource.getName()), is(empty()));
    assertThat(metaStore.getQueriesWithSink(dataSource.getName()), is(empty()));
  }

  @Test
  public void shouldDefaultToEmptySetOfQueriesForUnknownSource() {
    assertThat(metaStore.getQueriesWithSource("unknown"), is(empty()));
  }

  @Test
  public void shouldDefaultToEmptySetOfQueriesForUnknownSink() {
    assertThat(metaStore.getQueriesWithSink("unknown"), is(empty()));
  }

  @Test
  public void shouldRegisterQuerySources() {
    // Given:
    metaStore.putSource(dataSource);

    // When:
    metaStore.updateForPersistentQuery(
        "some query",
        ImmutableSet.of(dataSource.getName()),
        ImmutableSet.of());

    // Then:
    assertThat(metaStore.getQueriesWithSource(dataSource.getName()), contains("some query"));
  }

  @Test
  public void shouldRegisterQuerySinks() {
    // Given:
    metaStore.putSource(dataSource);

    // When:
    metaStore.updateForPersistentQuery(
        "some query",
        ImmutableSet.of(),
        ImmutableSet.of(dataSource.getName()));

    // Then:
    assertThat(metaStore.getQueriesWithSink(dataSource.getName()), contains("some query"));
  }

  @Test
  public void shouldBeThreadSafe() {
    IntStream.range(0, 1_000)
        .parallel()
        .forEach(idx -> {
          final KsqlTopic topic = mock(KsqlTopic.class);
          when(topic.getKsqlTopicName()).thenReturn("topic" + idx);
          metaStore.putTopic(topic);
          metaStore.getTopic(topic.getKsqlTopicName());

          final DataSource<?> source = mock(DataSource.class);
          when(source.getName()).thenReturn("source" + idx);
          metaStore.putSource(source);
          metaStore.getSource(source.getName());

          metaStore.getAllDataSources();

          final String queryId = "query" + idx;
          metaStore.updateForPersistentQuery(
              queryId,
              ImmutableSet.of(source.getName()),
              ImmutableSet.of(source.getName()));

          metaStore.getQueriesWithSource(source.getName());
          metaStore.getQueriesWithSink(source.getName());

          metaStore.copy();

          metaStore.removePersistentQuery(queryId);
          metaStore.deleteTopic(topic.getKsqlTopicName());
          metaStore.deleteSource(source.getName());
        });

    assertThat(metaStore.getAllKsqlTopics().keySet(), is(empty()));
    assertThat(metaStore.getAllDataSources().keySet(), is(empty()));
  }

  @Test(timeout = 10_000)
  public void shouldBeThreadSafeAroundRefIntegrity() throws Exception {
    // Given:
    final int iterations = 1_000;
    final AtomicInteger remaining = new AtomicInteger(iterations);
    final Set<String> sources = ImmutableSet.of(dataSource1.getName(), dataSource.getName());

    metaStore.putSource(dataSource1);

    final Future<?> mainThread = executor.submit(() -> {
      while (remaining.get() > 0) {
        metaStore.putSource(dataSource);

        while (true) {
          try {
            metaStore.deleteSource(dataSource.getName());
            break;
          } catch (final KsqlReferentialIntegrityException e) {
            // Expected
          }
        }
      }
    });

    // When:
    IntStream.range(0, iterations)
        .parallel()
        .forEach(idx -> {
          try {
            final String queryId = "query" + idx;

            while (true) {
              try {
                metaStore.updateForPersistentQuery(queryId, sources, sources);
                break;
              } catch (final KsqlException e) {
                // Expected
              }
            }

            assertThat(metaStore.getQueriesWithSource(dataSource.getName()), hasItem(queryId));
            assertThat(metaStore.getQueriesWithSink(dataSource.getName()), hasItem(queryId));

            metaStore.removePersistentQuery(queryId);

            remaining.decrementAndGet();
          } catch (final Throwable t) {
            remaining.set(0);
            throw t;
          }
        });

    // Then:
    assertThat(metaStore.getQueriesWithSource(dataSource1.getName()), is(empty()));
    assertThat(metaStore.getQueriesWithSink(dataSource1.getName()), is(empty()));
    mainThread.get(1, TimeUnit.MINUTES);
  }
}