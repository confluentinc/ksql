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

package io.confluent.ksql.physical.pull.operators;

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
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.KeyConstraints.KeyConstraint;
import io.confluent.ksql.planner.plan.PullFilterNode.WindowBounds;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
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
  private GenericKey KEY1;
  @Mock
  private GenericKey KEY2;
  @Mock
  private GenericKey KEY3;
  @Mock
  private GenericKey KEY4;
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
  @Mock
  private KeyConstraint keyConstraint1;
  @Mock
  private KeyConstraint keyConstraint2;
  @Mock
  private KeyConstraint keyConstraint3;
  @Mock
  private KeyConstraint keyConstraint4;

  @Before
  public void setUp() {
    when(keyConstraint1.getKey()).thenReturn(KEY1);
    when(keyConstraint2.getKey()).thenReturn(KEY2);
    when(keyConstraint3.getKey()).thenReturn(KEY3);
    when(keyConstraint4.getKey()).thenReturn(KEY4);
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
        materialization, logicalNode,
        ImmutableList.of(keyConstraint1, keyConstraint2, keyConstraint3, keyConstraint4));
    when(keyConstraint1.getWindowBounds()).thenReturn(Optional.of(windowBounds1));
    when(keyConstraint2.getWindowBounds()).thenReturn(Optional.of(windowBounds1));
    when(keyConstraint3.getWindowBounds()).thenReturn(Optional.of(windowBounds1));
    when(keyConstraint4.getWindowBounds()).thenReturn(Optional.of(windowBounds1));
    when(windowBounds1.getMergedStart()).thenReturn(WINDOW_START_BOUNDS);
    when(windowBounds1.getMergedEnd()).thenReturn(WINDOW_END_BOUNDS);
    when(materialization.windowed()).thenReturn(windowedTable);
    when(materialization.windowed().get(KEY1, 1, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS))
        .thenReturn(ImmutableList.of(WINDOWED_ROW1, WINDOWED_ROW2));
    when(materialization.windowed().get(KEY2, 2, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS))
        .thenReturn(Collections.emptyList());
    when(materialization.windowed().get(KEY3, 3, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS))
        .thenReturn(ImmutableList.of(WINDOWED_ROW3));
    when(materialization.windowed().get(KEY4, 3, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS))
        .thenReturn(ImmutableList.of(WINDOWED_ROW2, WINDOWED_ROW4));
    lookupOperator.setPartitionLocations(singleKeyPartitionLocations);
    lookupOperator.open();

    //Then:
    assertThat(lookupOperator.next(), is(WINDOWED_ROW1));
    assertThat(lookupOperator.next(), is(WINDOWED_ROW2));
    assertThat(lookupOperator.next(), is(WINDOWED_ROW3));
    assertThat(lookupOperator.next(), is(WINDOWED_ROW2));
    assertThat(lookupOperator.next(), is(WINDOWED_ROW4));
    assertThat(lookupOperator.next(), is(nullValue()));
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
        materialization, logicalNode,
        ImmutableList.of(keyConstraint1, keyConstraint2, keyConstraint3, keyConstraint4));
    when(keyConstraint1.getWindowBounds()).thenReturn(Optional.of(windowBounds1));
    when(keyConstraint2.getWindowBounds()).thenReturn(Optional.of(windowBounds1));
    when(keyConstraint3.getWindowBounds()).thenReturn(Optional.of(windowBounds1));
    when(keyConstraint4.getWindowBounds()).thenReturn(Optional.of(windowBounds1));
    when(windowBounds1.getMergedStart()).thenReturn(WINDOW_START_BOUNDS);
    when(windowBounds1.getMergedEnd()).thenReturn(WINDOW_END_BOUNDS);
    when(materialization.windowed()).thenReturn(windowedTable);
    when(materialization.windowed().get(KEY1, 1, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS))
        .thenReturn(ImmutableList.of(WINDOWED_ROW1, WINDOWED_ROW2));
    when(materialization.windowed().get(KEY2, 1, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS))
        .thenReturn(Collections.emptyList());
    when(materialization.windowed().get(KEY3, 3, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS))
        .thenReturn(ImmutableList.of(WINDOWED_ROW3));
    when(materialization.windowed().get(KEY4, 3, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS))
        .thenReturn(ImmutableList.of(WINDOWED_ROW2, WINDOWED_ROW4));
    lookupOperator.setPartitionLocations(multipleKeysPartitionLocations);
    lookupOperator.open();

    //Then:
    assertThat(lookupOperator.next(), is(WINDOWED_ROW1));
    assertThat(lookupOperator.next(), is(WINDOWED_ROW2));
    assertThat(lookupOperator.next(), is(WINDOWED_ROW3));
    assertThat(lookupOperator.next(), is(WINDOWED_ROW2));
    assertThat(lookupOperator.next(), is(WINDOWED_ROW4));
    assertThat(lookupOperator.next(), is(nullValue()));
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
        materialization, logicalNode,
        ImmutableList.of(keyConstraint1, keyConstraint2, keyConstraint3, keyConstraint4));
    when(keyConstraint1.getWindowBounds()).thenReturn(Optional.of(windowBounds1));
    when(keyConstraint2.getWindowBounds()).thenReturn(Optional.of(windowBounds2));
    when(keyConstraint3.getWindowBounds()).thenReturn(Optional.of(windowBounds3));
    when(keyConstraint4.getWindowBounds()).thenReturn(Optional.of(windowBounds4));
    when(windowBounds1.getMergedStart()).thenReturn(WINDOW_START_BOUNDS);
    when(windowBounds1.getMergedEnd()).thenReturn(WINDOW_END_BOUNDS);
    when(windowBounds2.getMergedStart()).thenReturn(Range.all());
    when(windowBounds2.getMergedEnd()).thenReturn(WINDOW_END_BOUNDS);
    when(windowBounds3.getMergedStart()).thenReturn(WINDOW_START_BOUNDS);
    when(windowBounds3.getMergedEnd()).thenReturn(Range.all());
    when(windowBounds4.getMergedStart()).thenReturn(Range.all());
    when(windowBounds4.getMergedEnd()).thenReturn(Range.all());
    when(materialization.windowed()).thenReturn(windowedTable);
    when(materialization.windowed().get(KEY1, 1, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS))
        .thenReturn(ImmutableList.of(WINDOWED_ROW1, WINDOWED_ROW2));
    when(materialization.windowed().get(KEY2, 2, Range.all(), WINDOW_END_BOUNDS))
        .thenReturn(Collections.emptyList());
    when(materialization.windowed().get(KEY3, 3, WINDOW_START_BOUNDS, Range.all()))
        .thenReturn(ImmutableList.of(WINDOWED_ROW3));
    when(materialization.windowed().get(KEY4, 3, Range.all(), Range.all()))
        .thenReturn(ImmutableList.of(WINDOWED_ROW2, WINDOWED_ROW4));
    lookupOperator.setPartitionLocations(singleKeyPartitionLocations);
    lookupOperator.open();

    //Then:
    assertThat(lookupOperator.next(), is(WINDOWED_ROW1));
    assertThat(lookupOperator.next(), is(WINDOWED_ROW2));
    assertThat(lookupOperator.next(), is(WINDOWED_ROW3));
    assertThat(lookupOperator.next(), is(WINDOWED_ROW2));
    assertThat(lookupOperator.next(), is(WINDOWED_ROW4));
    assertThat(lookupOperator.next(), is(nullValue()));
  }
}
