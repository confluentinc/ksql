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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.test.util.OptionalMatchers;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
  public void shouldDeepCopyLinkedAndReferentialSourcesOnCopy() {
    final DataSource dataSource2 = mock(DataSource.class);
    when(dataSource2.getName()).thenReturn(SourceName.of("dataSource2"));

    // Given:
    metaStore.putSource(dataSource, false);
    metaStore.putSource(dataSource1, false);
    metaStore.putSource(dataSource2, false);
    metaStore.addSourceReferences(dataSource1.getName(),
        Collections.singleton(dataSource.getName()));

    // When:
    final MetaStoreImpl copy = (MetaStoreImpl) metaStore.copy();
    metaStore.deleteSource(dataSource1.getName());
    metaStore.addSourceReferences(dataSource2.getName(),
        Collections.singleton(dataSource.getName()));

    // Then:
    assertThat(copy.getSourceConstraints(dataSource.getName()),
        hasItem(dataSource1.getName()));
    assertThat(metaStore.getSourceConstraints(dataSource.getName()),
        hasItem(dataSource2.getName()));
    assertThat(copy.getSourceReferences(dataSource1.getName()),
        hasItem(dataSource.getName()));
    assertThat(metaStore.getSourceReferences(dataSource2.getName()),
        hasItem(dataSource.getName()));
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
  public void shouldThrowOnLinkEqualSources() {
    // Given:
    metaStore.putSource(dataSource, false);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> metaStore.addSourceReferences(dataSource.getName(),
            Collections.singleton(dataSource.getName()))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Source name 'some source' should not be referenced itself."));
  }

  @Test
  public void shouldThrowOnLinkSourceWithUnknownSourceName() {
    // Given:
    metaStore.putSource(dataSource, false);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> metaStore.addSourceReferences(SourceName.of("s1"),
            Collections.singleton(dataSource.getName()))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "No data source with name 's1' exists."));
  }

  @Test
  public void shouldThrowOnLinkSourceWithUnknownLinkedSource() {
    // Given:
    metaStore.putSource(dataSource, false);

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> metaStore.addSourceReferences(dataSource.getName(),
            Collections.singleton(SourceName.of("s1")))
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "No data source with name 's1' exists."));
  }

  @Test
  public void shouldThrowOnDeleteSourceIfAnotherSourceIsLinked() {
    // Given:
    metaStore.putSource(dataSource, false);
    metaStore.putSource(dataSource1, false);
    metaStore.addSourceReferences(dataSource1.getName(),
        Collections.singleton(dataSource.getName()));

    // When:
    final Exception e = assertThrows(
        KsqlException.class,
        () -> metaStore.deleteSource(dataSource.getName())
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Cannot drop some source."));
    assertThat(e.getMessage(), containsString(
        "The following streams and/or tables read from this source: [some other source]."));
    assertThat(e.getMessage(), containsString(
        "You need to drop them before dropping some source."));
  }

  @Test
  public void shouldDeleteSourceAfterLinkedSourceIsDeleted() {
    // Given:
    metaStore.putSource(dataSource, false);
    metaStore.putSource(dataSource1, false);
    metaStore.addSourceReferences(dataSource1.getName(),
        Collections.singleton(dataSource.getName()));

    // When:
    metaStore.deleteSource(dataSource1.getName());
    metaStore.deleteSource(dataSource.getName());

    // Then:
    assertThat(metaStore.getSource(dataSource.getName()), is(nullValue()));
    assertThat(metaStore.getSource(dataSource1.getName()), is(nullValue()));
  }

  @Test
  public void shouldDeleteSourceIfAnotherSourceIsLinkedAndRestoreIsInProgress() {
    // Given:
    metaStore.putSource(dataSource, false);
    metaStore.putSource(dataSource1, false);
    metaStore.addSourceReferences(dataSource1.getName(),
        Collections.singleton(dataSource.getName()));

    // When:
    metaStore.deleteSource(dataSource.getName(), true);

    // Then:
    assertThat(metaStore.getSource(dataSource.getName()), is(nullValue()));
    assertThat(metaStore.getSource(dataSource1.getName()), is(dataSource1));

    // check that constraints are deleted, but references are not
    assertThat(metaStore.getSourceConstraints(dataSource.getName()),
        containsInAnyOrder(ImmutableSet.of().toArray()));
    assertThat(metaStore.getSourceReferences(dataSource1.getName()),
        containsInAnyOrder(ImmutableSet.of(dataSource.getName()).toArray()));
  }

  @Test
  public void shouldAddReferentialLinkOnPutSource() {
    // Given:
    metaStore.putSource(dataSource, false);
    metaStore.putSource(dataSource1, false);

    // this constraint cannot be added if none of the sources exist, so we added them first in the
    // prevous putSource() calls
    metaStore.addSourceReferences(dataSource1.getName(),
        Collections.singleton(dataSource.getName()));

    // now force source delete if constraint exists, which will leave dataSource1 with a stale
    // reference to dataSource
    metaStore.deleteSource(dataSource.getName(), true);

    // When:
    // quickly check the dataSource does not exist, then add it again
    assertThat(metaStore.getSource(dataSource.getName()), is(nullValue()));
    metaStore.putSource(dataSource, false);

    // Then:
    assertThat(metaStore.getSource(dataSource.getName()), is(dataSource));
    assertThat(metaStore.getSource(dataSource1.getName()), is(dataSource1));

    // check source constraints are put back, and references weren't removed
    assertThat(metaStore.getSourceConstraints(dataSource.getName()),
        containsInAnyOrder(ImmutableSet.of(dataSource1.getName()).toArray()));
    assertThat(metaStore.getSourceReferences(dataSource1.getName()),
        containsInAnyOrder(ImmutableSet.of(dataSource.getName()).toArray()));
  }

  @Test
  public void shouldReplaceSourceCopyLinkedAndReferentialSources() {
    // Given:
    metaStore.putSource(dataSource, false);
    metaStore.putSource(dataSource1, false);
    metaStore.addSourceReferences(dataSource1.getName(),
        Collections.singleton(dataSource.getName()));

    // When:
    metaStore.putSource(dataSource, true);

    // Then:
    assertThat(metaStore.getSourceConstraints(dataSource.getName()),
        hasItem(dataSource1.getName()));
    assertThat(metaStore.getSourceReferences(dataSource1.getName()),
        hasItem(dataSource.getName()));
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
          metaStore.copy();
          metaStore.deleteSource(source.getName());
        });

    assertThat(metaStore.getAllDataSources().keySet(), is(empty()));
  }
}