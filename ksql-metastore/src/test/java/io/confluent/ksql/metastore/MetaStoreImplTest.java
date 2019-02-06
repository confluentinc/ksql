/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlReferentialIntegrityException;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
@RunWith(MockitoJUnitRunner.class)
public class MetaStoreImplTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private KsqlTopic topic;
  @Mock
  private StructuredDataSource dataSource;
  private MetaStore metaStore;

  @Before
  public void setUp() {
    metaStore = new MetaStoreImpl(functionRegistry);

    when(topic.getName()).thenReturn("some registered topic");
    when(dataSource.getName()).thenReturn("some source");
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  @Test
  public void shouldCopyFunctionRegistryOnCopy() {
    // Given:
    final FunctionRegistry functionRegistryCopy = mock(FunctionRegistry.class);
    when(functionRegistry.copy()).thenReturn(functionRegistryCopy);

    // When:
    final MetaStore copy = metaStore.copy();
    copy.listFunctions();

    // Then:
    verify(functionRegistryCopy).listFunctions();
    verify(functionRegistry, never()).listFunctions();
  }

  @Test
  public void shouldDeepCopyTopicsOnCopy() {
    // Given:
    metaStore.putTopic(topic);

    // When:
    final MetaStore copy = metaStore.copy();
    metaStore.deleteTopic(topic.getName());

    // Then:
    assertThat(copy.getAllKsqlTopics().keySet(), contains(topic.getName()));
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
    assertThat(copy.getAllStructuredDataSources().keySet(), contains(dataSource.getName()));
    assertThat(metaStore.getAllStructuredDataSources().keySet(), is(empty()));
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
  public void shouldNotAllowModificationViaGetAllStructuredDataSources() {
    // Given:
    metaStore.putSource(dataSource);

    final Map<String, StructuredDataSource> dataSources = metaStore
        .getAllStructuredDataSources();

    // When
    dataSources.keySet().clear();

    // Then:
    assertThat(metaStore.getAllStructuredDataSources().keySet(), contains(dataSource.getName()));
  }

  @Test
  public void shouldNotAllowModificationViaGetAllKsqlTopics() {
    // Given:
    metaStore.putTopic(topic);

    final Map<String, KsqlTopic> topics = metaStore.getAllKsqlTopics();

    // Expect:
    expectedException.expect(UnsupportedOperationException.class);

    // When
    topics.keySet().clear();
  }

  @Test
  public void shouldThrowOnDuplicateTopic() {
    // Given:
    metaStore.putTopic(topic);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Another topic with the same name already exists");

    // When:
    metaStore.putTopic(topic);
  }

  @Test
  public void shouldThrowOnRemoveUnknownTopic() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("No topic with name bob was registered");

    // When:
    metaStore.deleteTopic("bob");
  }

  @Test
  public void shouldThrowOnDuplicateSource() {
    // Given:
    metaStore.putSource(dataSource);

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Another data source with the same name already exists");

    // When:
    metaStore.putSource(dataSource);
  }

  @Test
  public void shouldThrowOnRemoveUnknownSource() {
    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("No data source with name bob exists");

    // When:
    metaStore.deleteSource("bob");
  }

  @Test
  public void shouldThrowOnDropSourceIfUsedAsSourceOfQueries() {
    // Given:
    metaStore.putSource(dataSource);
    metaStore.updateForPersistentQuery(
        "source query",
        ImmutableSet.of(dataSource.getName()),
        ImmutableSet.of());

    // Then:
    expectedException.expect(KsqlReferentialIntegrityException.class);
    expectedException.expectMessage("The following queries read from this source: [source query]");

    // When:
    metaStore.deleteSource(dataSource.getName());
  }

  @Test
  public void shouldThrowOnUpdateForPersistentQueryOnUnknownSource() {
    // Then:
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Unknown source: unknown");

    // When:
    metaStore.updateForPersistentQuery(
        "source query",
        ImmutableSet.of("unknown"),
        ImmutableSet.of());
  }

  @Test
  public void shouldThrowOnUpdateForPersistentQueryOnUnknownSink() {
    // Then:
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Unknown sink: unknown");

    // When:
    metaStore.updateForPersistentQuery(
        "sink query",
        ImmutableSet.of(),
        ImmutableSet.of("unknown"));
  }

  @Test
  public void shouldThrowOnDropSourceIfUsedAsSinkOfQueries() {
    // Given:
    metaStore.putSource(dataSource);
    metaStore.updateForPersistentQuery(
        "sink query",
        ImmutableSet.of(),
        ImmutableSet.of(dataSource.getName()));

    // Then:
    expectedException.expect(KsqlReferentialIntegrityException.class);
    expectedException.expectMessage("The following queries write into this source: [sink query]");

    // When:
    metaStore.deleteSource(dataSource.getName());
  }

  @Test
  public void shouldBeThreadSafe() {
    IntStream.range(0, 5_000)
        .parallel()
        .forEach(idx -> {
          final KsqlTopic topic = mock(KsqlTopic.class);
          when(topic.getName()).thenReturn("topic" + idx);
          metaStore.putTopic(topic);
          metaStore.getTopic(topic.getName());

          final StructuredDataSource source = mock(StructuredDataSource.class);
          when(source.getName()).thenReturn("source" + idx);
          metaStore.putSource(source);
          metaStore.getSource(source.getName());

          metaStore.getAllStructuredDataSources();

          final String queryId = "query" + idx;
          metaStore.updateForPersistentQuery(
              queryId,
              ImmutableSet.of(source.getName()),
              ImmutableSet.of(source.getName()));

          metaStore.getQueriesWithSource(source.getName());
          metaStore.getQueriesWithSink(source.getName());

          metaStore.copy();

          metaStore.removePersistentQuery(queryId);
          metaStore.deleteTopic(topic.getName());
          metaStore.deleteSource(source.getName());
        });

    assertThat(metaStore.getAllKsqlTopics().keySet(), is(empty()));
    assertThat(metaStore.getAllStructuredDataSources().keySet(), is(empty()));
  }
}