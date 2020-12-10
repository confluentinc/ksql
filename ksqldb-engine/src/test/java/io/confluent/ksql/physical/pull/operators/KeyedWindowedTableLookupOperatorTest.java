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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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
  private WhereInfo.WindowBounds windowBounds;

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

    final KeyedWindowedTableLookupOperator lookupOperator = new KeyedWindowedTableLookupOperator(materialization, logicalNode, windowBounds);
    when(windowBounds.getStart()).thenReturn(WINDOW_START_BOUNDS);
    when(windowBounds.getEnd()).thenReturn(WINDOW_END_BOUNDS);
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

    final KeyedWindowedTableLookupOperator lookupOperator = new KeyedWindowedTableLookupOperator(materialization, logicalNode, windowBounds);
    when(windowBounds.getStart()).thenReturn(WINDOW_START_BOUNDS);
    when(windowBounds.getEnd()).thenReturn(WINDOW_END_BOUNDS);
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
}
