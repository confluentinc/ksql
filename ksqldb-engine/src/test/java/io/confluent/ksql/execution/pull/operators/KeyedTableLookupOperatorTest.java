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
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlPartitionLocation;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.execution.streams.materialization.MaterializedTable;
import io.confluent.ksql.execution.streams.materialization.Row;
import io.confluent.ksql.execution.streams.materialization.ks.KsLocator;
import io.confluent.ksql.execution.common.QueryRow;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.KeyConstraint;
import io.confluent.ksql.planner.plan.KeyConstraint.ConstraintOperator;
import io.confluent.ksql.util.IteratorUtil;
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
public class KeyedTableLookupOperatorTest {

  @Mock
  private KsqlNode node1;
  @Mock
  private KsqlNode node2;
  @Mock
  private KsqlNode node3;
  @Mock
  private Materialization materialization;
  @Mock
  private MaterializedTable nonWindowedTable;
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
  private Row ROW1;
  @Mock
  private Row ROW3;
  @Mock
  private Row ROW4;

  @Before
  public void setUp() {
    when(KEY1.getKey()).thenReturn(GKEY1);
    when(KEY2.getKey()).thenReturn(GKEY2);
    when(KEY3.getKey()).thenReturn(GKEY3);
    when(KEY4.getKey()).thenReturn(GKEY4);
    when(KEY1.getOperator()).thenReturn(ConstraintOperator.EQUAL);
    when(KEY2.getOperator()).thenReturn(ConstraintOperator.EQUAL);
    when(KEY3.getOperator()).thenReturn(ConstraintOperator.EQUAL);
    when(KEY4.getOperator()).thenReturn(ConstraintOperator.EQUAL);
    when(ROW1.key()).thenReturn(GKEY1);
    when(ROW3.key()).thenReturn(GKEY3);
    when(ROW4.key()).thenReturn(GKEY4);
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

    final KeyedTableLookupOperator lookupOperator = new KeyedTableLookupOperator(
        materialization, logicalNode, Optional.empty());
    when(materialization.nonWindowed()).thenReturn(nonWindowedTable);
    when(materialization.nonWindowed().get(GKEY1, 1, Optional.empty())).thenReturn(
        IteratorUtil.of(ROW1));
    when(materialization.nonWindowed().get(GKEY2, 2, Optional.empty())).thenReturn(
        IteratorUtil.of());
    when(materialization.nonWindowed().get(GKEY3, 3, Optional.empty())).thenReturn(
        IteratorUtil.of(ROW3));
    when(materialization.nonWindowed().get(GKEY4, 3, Optional.empty())).thenReturn(
        IteratorUtil.of(ROW4));


    lookupOperator.setPartitionLocations(singleKeyPartitionLocations);
    lookupOperator.open();

    //Then:
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY1));
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY3));
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY4));
    assertThat(lookupOperator.next(), is(nullValue()));
    assertThat(lookupOperator.getReturnedRowCount(), is(3L));
  }

  @Test
  public void shouldLookupRowsForMultipleKeys() {
    //Given:
    final List<KsqlPartitionLocation> multipleKeysPartitionLocations = new ArrayList<>();
    multipleKeysPartitionLocations.add(new KsLocator.PartitionLocation(
        Optional.of(ImmutableSet.of(KEY1, KEY2)), 1, ImmutableList.of(node1)));
    multipleKeysPartitionLocations.add(new KsLocator.PartitionLocation(
        Optional.of(ImmutableSet.of(KEY3, KEY4)), 3, ImmutableList.of(node3)));

    final KeyedTableLookupOperator lookupOperator = new KeyedTableLookupOperator(
        materialization, logicalNode, Optional.empty());
    when(materialization.nonWindowed()).thenReturn(nonWindowedTable);
    when(materialization.nonWindowed().get(GKEY1, 1, Optional.empty())).thenReturn(
        IteratorUtil.of(ROW1));
    when(materialization.nonWindowed().get(GKEY2, 1, Optional.empty())).thenReturn(
        Collections.emptyIterator());
    when(materialization.nonWindowed().get(GKEY3, 3, Optional.empty())).thenReturn(
        IteratorUtil.of(ROW3));
    when(materialization.nonWindowed().get(GKEY4, 3, Optional.empty())).thenReturn(
        IteratorUtil.of(ROW4));
    lookupOperator.setPartitionLocations(multipleKeysPartitionLocations);
    lookupOperator.open();

    //Then:
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY1));
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY3));
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY4));
    assertThat(lookupOperator.next(), is(nullValue()));
    assertThat(lookupOperator.getReturnedRowCount(), is(3L));
  }

  @Test
  public void shouldLookupRowsForRangeKeySinglePartition() {
    //Given:
    when(KEY3.getOperator()).thenReturn(ConstraintOperator.LESS_THAN_OR_EQUAL);
    final List<KsqlPartitionLocation> singleKeyPartitionLocations = new ArrayList<>();
    singleKeyPartitionLocations.add(new KsLocator.PartitionLocation(
      Optional.of(ImmutableSet.of(KEY1, KEY3)), 1, ImmutableList.of(node1)));

    final KeyedTableLookupOperator lookupOperator = new KeyedTableLookupOperator(
        materialization, logicalNode, Optional.empty());
    when(materialization.nonWindowed()).thenReturn(nonWindowedTable);
    when(materialization.nonWindowed().get(GKEY1, 1, Optional.empty())).thenReturn(
        Collections.emptyIterator());
    when(materialization.nonWindowed().get(1,null, GKEY3, Optional.empty())).thenReturn(
        IteratorUtil.of(ROW1, ROW3));

    lookupOperator.setPartitionLocations(singleKeyPartitionLocations);
    lookupOperator.open();

    //Then:
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY1));
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY3));
    assertThat(lookupOperator.getReturnedRowCount(), is(2L));
  }

  @Test
  public void shouldLookupRowsForRangeKeyMultiplePartitions() {
    //Given:
    when(KEY2.getOperator()).thenReturn(ConstraintOperator.LESS_THAN_OR_EQUAL);
    when(KEY3.getOperator()).thenReturn(ConstraintOperator.LESS_THAN_OR_EQUAL);
    final List<KsqlPartitionLocation> multipleKeysPartitionLocations = new ArrayList<>();
    multipleKeysPartitionLocations.add(new KsLocator.PartitionLocation(
      Optional.of(ImmutableSet.of(KEY1, KEY2)), 1, ImmutableList.of(node1)));
    multipleKeysPartitionLocations.add(new KsLocator.PartitionLocation(
      Optional.of(ImmutableSet.of(KEY3, KEY4)), 3, ImmutableList.of(node3)));

    final KeyedTableLookupOperator lookupOperator = new KeyedTableLookupOperator(
        materialization, logicalNode, Optional.empty());
    when(materialization.nonWindowed()).thenReturn(nonWindowedTable);
    when(materialization.nonWindowed().get(GKEY1, 1, Optional.empty())).thenReturn(
        Collections.emptyIterator());
    when(materialization.nonWindowed().get(1,null, GKEY2, Optional.empty())).thenReturn(
        IteratorUtil.of(ROW1));
    when(materialization.nonWindowed().get(3,null, GKEY3, Optional.empty())).thenReturn(
        IteratorUtil.of(ROW3));
    when(materialization.nonWindowed().get(GKEY4, 3, Optional.empty())).thenReturn(
        Collections.emptyIterator());

    lookupOperator.setPartitionLocations(multipleKeysPartitionLocations);
    lookupOperator.open();

    //Then:
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY1));
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY3));
    assertThat(lookupOperator.next(), is(nullValue()));
    assertThat(lookupOperator.getReturnedRowCount(), is(2L));
  }
}
