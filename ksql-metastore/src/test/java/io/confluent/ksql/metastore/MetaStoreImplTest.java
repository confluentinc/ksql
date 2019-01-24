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
import io.confluent.ksql.function.FunctionRegistry;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

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
}