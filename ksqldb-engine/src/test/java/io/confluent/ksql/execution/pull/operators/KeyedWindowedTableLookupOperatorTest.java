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

package io.confluent.ksql.execution.pull.operators;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlPartitionLocation;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.execution.streams.materialization.MaterializedWindowedTable;
import io.confluent.ksql.execution.streams.materialization.WindowedRow;
import io.confluent.ksql.execution.streams.materialization.ks.KsLocator;
import io.confluent.ksql.execution.common.QueryRow;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.KeyConstraint;
import io.confluent.ksql.planner.plan.QueryFilterNode.WindowBounds;
import io.confluent.ksql.util.IteratorUtil;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KeyedWindowedTableLookupOperatorTest {

  @Mock
  private KsqlNode node1;
  @Mock
  private KsqlNode node2;
  @Mock
  private KsqlNode node3;
  @Mock
  private Materialization materialization;
  @Mock
  private DataSourceNode logicalNode;
  @Mock
  private KeyConstraint KEY1;
  @Mock
  private KeyConstraint KEY2;
  @Mock
  private KeyConstraint KEY3;
  @Mock
  private KeyConstraint KEY4;
  @Mock
  private GenericKey GKEY1;
  @Mock
  private GenericKey GKEY2;
  @Mock
  private GenericKey GKEY3;
  @Mock
  private GenericKey GKEY4;
  @Mock
  private WindowedRow WINDOWED_ROW1;
  @Mock
  private WindowedRow WINDOWED_ROW2;
  @Mock
  private WindowedRow WINDOWED_ROW3;
  @Mock
  private WindowedRow WINDOWED_ROW4;
  @Mock
  private MaterializedWindowedTable windowedTable;
  @Mock
  private Range<Instant> WINDOW_START_BOUNDS;
  @Mock
  private Range<Instant> WINDOW_END_BOUNDS;
  @Mock
  private WindowBounds windowBounds1;
  @Mock
  private WindowBounds windowBounds2;
  @Mock
  private WindowBounds windowBounds3;
  @Mock
  private WindowBounds windowBounds4;

  @Before
  public void setUp() {
    when(KEY1.getKey()).thenReturn(GKEY1);
    when(KEY2.getKey()).thenReturn(GKEY2);
    when(KEY3.getKey()).thenReturn(GKEY3);
    when(KEY4.getKey()).thenReturn(GKEY4);
    when(KEY1.getWindowBounds()).thenReturn(Optional.of(windowBounds1));
    when(KEY2.getWindowBounds()).thenReturn(Optional.of(windowBounds1));
    when(KEY3.getWindowBounds()).thenReturn(Optional.of(windowBounds1));
    when(KEY4.getWindowBounds()).thenReturn(Optional.of(windowBounds1));
    when(windowBounds1.getMergedStart()).thenReturn(WINDOW_START_BOUNDS);
    when(windowBounds1.getMergedEnd()).thenReturn(WINDOW_END_BOUNDS);
    when(WINDOWED_ROW1.key()).thenReturn(GKEY1);
    when(WINDOWED_ROW2.key()).thenReturn(GKEY2);
    when(WINDOWED_ROW3.key()).thenReturn(GKEY3);
    when(WINDOWED_ROW4.key()).thenReturn(GKEY4);
  }

  @Test
  public void shouldLookupRowsForSingleKey() {
    //Given:
    final List<KsqlPartitionLocation> singleKeyPartitionLocations = new ArrayList<>();
    singleKeyPartitionLocations.add(new KsLocator.PartitionLocation(
        Optional.of(ImmutableSet.of(KEY1)), 1, ImmutableList.of(node1)));
    singleKeyPartitionLocations.add(new KsLocator.PartitionLocation(
        Optional.of(ImmutableSet.of(KEY2)), 2, ImmutableList.of(node2)));
    singleKeyPartitionLocations.add(new KsLocator.PartitionLocation(
        Optional.of(ImmutableSet.of(KEY3)), 3, ImmutableList.of(node3)));
    singleKeyPartitionLocations.add(new KsLocator.PartitionLocation(
        Optional.of(ImmutableSet.of(KEY4)), 3, ImmutableList.of(node3)));

    final KeyedWindowedTableLookupOperator lookupOperator = new KeyedWindowedTableLookupOperator(
        materialization, logicalNode, Optional.empty());
    when(materialization.windowed()).thenReturn(windowedTable);
    when(materialization.windowed().get(GKEY1, 1, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS, Optional.empty()))
        .thenReturn(IteratorUtil.of(WINDOWED_ROW1, WINDOWED_ROW2));
    when(materialization.windowed().get(GKEY2, 2, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS, Optional.empty()))
        .thenReturn(IteratorUtil.of());
    when(materialization.windowed().get(GKEY3, 3, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS, Optional.empty()))
        .thenReturn(IteratorUtil.of(WINDOWED_ROW3));
    when(materialization.windowed().get(GKEY4, 3, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS, Optional.empty()))
        .thenReturn(IteratorUtil.of(WINDOWED_ROW2, WINDOWED_ROW4));
    lookupOperator.setPartitionLocations(singleKeyPartitionLocations);
    lookupOperator.open();

    //Then:
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY1));
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY2));
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY3));
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY2));
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY4));
    assertThat(lookupOperator.next(), is(nullValue()));
    assertThat(lookupOperator.getReturnedRowCount(), is(5L));
  }

  @Test
  public void shouldLookupRowsForMultipleKey() {
    //Given:
    final List<KsqlPartitionLocation> multipleKeysPartitionLocations = new ArrayList<>();
    multipleKeysPartitionLocations.add(new KsLocator.PartitionLocation(
        Optional.of(ImmutableSet.of(KEY1, KEY2)), 1, ImmutableList.of(node1)));
    multipleKeysPartitionLocations.add(new KsLocator.PartitionLocation(
        Optional.of(ImmutableSet.of(KEY3, KEY4)), 3, ImmutableList.of(node3)));

    final KeyedWindowedTableLookupOperator lookupOperator = new KeyedWindowedTableLookupOperator(
        materialization, logicalNode, Optional.empty());
    when(windowBounds1.getMergedStart()).thenReturn(WINDOW_START_BOUNDS);
    when(windowBounds1.getMergedEnd()).thenReturn(WINDOW_END_BOUNDS);
    when(materialization.windowed()).thenReturn(windowedTable);
    when(materialization.windowed().get(GKEY1, 1, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS, Optional.empty()))
        .thenReturn(IteratorUtil.of(WINDOWED_ROW1, WINDOWED_ROW2));
    when(materialization.windowed().get(GKEY2, 1, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS, Optional.empty()))
        .thenReturn(IteratorUtil.of());
    when(materialization.windowed().get(GKEY3, 3, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS, Optional.empty()))
        .thenReturn(IteratorUtil.of(WINDOWED_ROW3));
    when(materialization.windowed().get(GKEY4, 3, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS, Optional.empty()))
        .thenReturn(IteratorUtil.of(WINDOWED_ROW2, WINDOWED_ROW4));
    lookupOperator.setPartitionLocations(multipleKeysPartitionLocations);
    lookupOperator.open();

    //Then:
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY1));
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY2));
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY3));
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY2));
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY4));
    assertThat(lookupOperator.next(), is(nullValue()));
    assertThat(lookupOperator.getReturnedRowCount(), is(5L));
  }

  @Test
  public void shouldUseDifferentWindowBoundsPerKey() {
    //Given:
    final List<KsqlPartitionLocation> singleKeyPartitionLocations = new ArrayList<>();
    singleKeyPartitionLocations.add(new KsLocator.PartitionLocation(
        Optional.of(ImmutableSet.of(KEY1)), 1, ImmutableList.of(node1)));
    singleKeyPartitionLocations.add(new KsLocator.PartitionLocation(
        Optional.of(ImmutableSet.of(KEY2)), 2, ImmutableList.of(node2)));
    singleKeyPartitionLocations.add(new KsLocator.PartitionLocation(
        Optional.of(ImmutableSet.of(KEY3)), 3, ImmutableList.of(node3)));
    singleKeyPartitionLocations.add(new KsLocator.PartitionLocation(
        Optional.of(ImmutableSet.of(KEY4)), 3, ImmutableList.of(node3)));

    final KeyedWindowedTableLookupOperator lookupOperator = new KeyedWindowedTableLookupOperator(
        materialization, logicalNode, Optional.empty());
    when(KEY1.getWindowBounds()).thenReturn(Optional.of(windowBounds1));
    when(KEY2.getWindowBounds()).thenReturn(Optional.of(windowBounds2));
    when(KEY3.getWindowBounds()).thenReturn(Optional.of(windowBounds3));
    when(KEY4.getWindowBounds()).thenReturn(Optional.of(windowBounds4));
    when(windowBounds1.getMergedStart()).thenReturn(WINDOW_START_BOUNDS);
    when(windowBounds1.getMergedEnd()).thenReturn(WINDOW_END_BOUNDS);
    when(windowBounds2.getMergedStart()).thenReturn(Range.all());
    when(windowBounds2.getMergedEnd()).thenReturn(WINDOW_END_BOUNDS);
    when(windowBounds3.getMergedStart()).thenReturn(WINDOW_START_BOUNDS);
    when(windowBounds3.getMergedEnd()).thenReturn(Range.all());
    when(windowBounds4.getMergedStart()).thenReturn(Range.all());
    when(windowBounds4.getMergedEnd()).thenReturn(Range.all());
    when(materialization.windowed()).thenReturn(windowedTable);
    when(materialization.windowed().get(GKEY1, 1, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS, Optional.empty()))
        .thenReturn(IteratorUtil.of(WINDOWED_ROW1, WINDOWED_ROW2));
    when(materialization.windowed().get(GKEY2, 2, Range.all(), WINDOW_END_BOUNDS, Optional.empty()))
        .thenReturn(IteratorUtil.of());
    when(materialization.windowed().get(GKEY3, 3, WINDOW_START_BOUNDS, Range.all(), Optional.empty()))
        .thenReturn(IteratorUtil.of(WINDOWED_ROW3));
    when(materialization.windowed().get(GKEY4, 3, Range.all(), Range.all(), Optional.empty()))
        .thenReturn(IteratorUtil.of(WINDOWED_ROW2, WINDOWED_ROW4));
    lookupOperator.setPartitionLocations(singleKeyPartitionLocations);
    lookupOperator.open();

    //Then:
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY1));
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY2));
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY3));
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY2));
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY4));
    assertThat(lookupOperator.next(), is(nullValue()));
    assertThat(lookupOperator.getReturnedRowCount(), is(5L));
  }
}
