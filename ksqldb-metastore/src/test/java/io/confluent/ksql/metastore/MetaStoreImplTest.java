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

import static io.confluent.ksql.name.SourceName.of;
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
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.MetaStoreMatchers.OptionalMatchers;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlReferentialIntegrityException;
import java.util.Map;
import java.util.Optional;
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
  private DataSource dataSource;
  @Mock
  private DataSource dataSource1;
  private MetaStoreImpl metaStore;
  private ExecutorService executor;

  private static final SourceName SOME_SOURCE = SourceName.of("some source");
  private static final SourceName SOME_OTHER_SOURCE = SourceName.of("some other source");
  private static final SourceName UNKNOWN_SOURCE = SourceName.of("unknown");

  @Before
  public void setUp() {
    metaStore = new MetaStoreImpl(functionRegistry);

    when(dataSource.getName()).thenReturn(SOME_SOURCE);
    when(dataSource.getDataSourceType()).thenReturn(DataSourceType.KTABLE);
    when(dataSource1.getName()).thenReturn(SOME_OTHER_SOURCE);

    executor = Executors.newSingleThreadExecutor();
  }

  @After
  public void tearDown() throws Exception {
    executor.shutdownNow();
    executor.awaitTermination(1, TimeUnit.MINUTES);
  }

  @Test
  public void shouldDeepCopySourcesOnCopy() {
    // Given:
    metaStore.putSource(dataSource, false);

    // When:
    final MetaStore copy = metaStore.copy();
    metaStore.deleteSource(dataSource.getName());

    // Then:
    assertThat(copy.getAllDataSources().keySet(), contains(dataSource.getName()));
    assertThat(metaStore.getAllDataSources().keySet(), is(empty()));
  }

  @Test
  public void shouldDeepCopyTypesOnCopy() {
    // Given:
    metaStore.registerType("foo", SqlPrimitiveType.of(SqlBaseType.STRING));

    // When:
    final MetaStore copy = metaStore.copy();
    metaStore.deleteType("foo");

    // Then:
    assertThat(copy.resolveType("foo"), OptionalMatchers.of(is(SqlPrimitiveType.of(SqlBaseType.STRING))));
    assertThat("Expected no types in the original", !metaStore.types().hasNext());
  }

  @Test
  public void shouldDeepCopySourceReferentialIntegrityDataOnCopy() {
    // Given:
    metaStore.putSource(dataSource, false);

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
    metaStore.putSource(dataSource, false);

    final Map<SourceName, DataSource> dataSources = metaStore
        .getAllDataSources();

    // When
    dataSources.keySet().clear();

    // Then:
    assertThat(metaStore.getAllDataSources().keySet(), contains(dataSource.getName()));
  }

  @Test
  public void shouldThrowOnDuplicateSource() {
    // Given:
    metaStore.putSource(dataSource, false);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> metaStore.putSource(dataSource, false)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Cannot add table 'some source': A table with the same name already exists"));
  }

  @Test
  public void shouldThrowOnRemoveUnknownSource() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> metaStore.deleteSource(of("bob"))
    );

    // Then:
    assertThat(e.getMessage(), containsString("No data source with name bob exists"));
  }

  @Test
  public void shouldThrowOnDropSourceIfUsedAsSourceOfQueries() {
    // Given:
    metaStore.putSource(dataSource, false);
    metaStore.updateForPersistentQuery(
        "source query",
        ImmutableSet.of(dataSource.getName()),
        ImmutableSet.of());

    // When:
    final Exception e = assertThrows(
        KsqlReferentialIntegrityException.class,
        () -> metaStore.deleteSource(dataSource.getName())
    );

    // Then:
    assertThat(e.getMessage(), containsString("The following queries read from this source: [source query]"));
  }

  @Test
  public void shouldThrowOnUpdateForPersistentQueryOnUnknownSource() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> metaStore.updateForPersistentQuery(
            "source query",
            ImmutableSet.of(UNKNOWN_SOURCE),
            ImmutableSet.of())
    );

    // Then:
    assertThat(e.getMessage(), containsString("Unknown source: unknown"));
  }

  @Test
  public void shouldThrowOnUpdateForPersistentQueryOnUnknownSink() {
    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> metaStore.updateForPersistentQuery(
            "sink query",
            ImmutableSet.of(),
            ImmutableSet.of(UNKNOWN_SOURCE))
    );

    // Then:
    assertThat(e.getMessage(), containsString("Unknown source: unknown"));
  }

  @Test
  public void shouldThrowOnDropSourceIfUsedAsSinkOfQueries() {
    // Given:
    metaStore.putSource(dataSource, false);
    metaStore.updateForPersistentQuery(
        "sink query",
        ImmutableSet.of(),
        ImmutableSet.of(dataSource.getName()));

    // When:
    final Exception e = assertThrows(
        KsqlReferentialIntegrityException.class,
        () -> metaStore.deleteSource(dataSource.getName())
    );

    // Then:
    assertThat(e.getMessage(), containsString("The following queries write into this source: [sink query]"));
  }

  @Test
  public void shouldFailToUpdateForPersistentQueryAtomicallyForUnknownSource() {
    // When:
    try {
      metaStore.updateForPersistentQuery(
          "some query",
          ImmutableSet.of(dataSource.getName(), UNKNOWN_SOURCE),
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
          ImmutableSet.of(dataSource.getName(), UNKNOWN_SOURCE));
    } catch (final KsqlException e) {
      // Expected
    }

    // Then:
    assertThat(metaStore.getQueriesWithSource(dataSource.getName()), is(empty()));
    assertThat(metaStore.getQueriesWithSink(dataSource.getName()), is(empty()));
  }

  @Test
  public void shouldDefaultToEmptySetOfQueriesForUnknownSource() {
    assertThat(metaStore.getQueriesWithSource(UNKNOWN_SOURCE), is(empty()));
  }

  @Test
  public void shouldDefaultToEmptySetOfQueriesForUnknownSink() {
    assertThat(metaStore.getQueriesWithSink(UNKNOWN_SOURCE), is(empty()));
  }

  @Test
  public void shouldRegisterQuerySources() {
    // Given:
    metaStore.putSource(dataSource, false);

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
    metaStore.putSource(dataSource, false);

    // When:
    metaStore.updateForPersistentQuery(
        "some query",
        ImmutableSet.of(),
        ImmutableSet.of(dataSource.getName()));

    // Then:
    assertThat(metaStore.getQueriesWithSink(dataSource.getName()), contains("some query"));
  }

  @Test
  public void shouldRegisterType() {
    // Given:
    final SqlType type = SqlPrimitiveType.of(SqlBaseType.STRING);
    metaStore.registerType("foo", type);

    // When:
    final Optional<SqlType> resolved = metaStore.resolveType("foo");

    // Then:
    assertThat("expected to find type", resolved.isPresent());
    assertThat(resolved.get(), is(type));
  }

  @Test
  public void shouldNotFindUnregisteredType() {
    // When:
    final Optional<SqlType> resolved = metaStore.resolveType("foo");

    // Then:
    assertThat("expected not to find type", !resolved.isPresent());
  }

  @Test
  public void shouldBeThreadSafe() {
    IntStream.range(0, 1_000)
        .parallel()
        .forEach(idx -> {
          final DataSource source = mock(DataSource.class);
          when(source.getName()).thenReturn(SourceName.of("source" + idx));
          metaStore.putSource(source, false);
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
          metaStore.deleteSource(source.getName());
        });

    assertThat(metaStore.getAllDataSources().keySet(), is(empty()));
  }

  @Test(timeout = 10_000)
  public void shouldBeThreadSafeAroundRefIntegrity() throws Exception {
    // Given:
    final int iterations = 1_000;
    final AtomicInteger remaining = new AtomicInteger(iterations);
    final ImmutableSet<SourceName> sources = ImmutableSet.of(dataSource1.getName(), dataSource.getName());

    metaStore.putSource(dataSource1, false);

    final Future<?> mainThread = executor.submit(() -> {
      while (remaining.get() > 0) {
        metaStore.putSource(dataSource, false);

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