package io.confluent.ksql.execution.pull.operators;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlPartitionLocation;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.execution.streams.materialization.MaterializedTable;
import io.confluent.ksql.execution.streams.materialization.Row;
import io.confluent.ksql.execution.streams.materialization.ks.KsLocator;
import io.confluent.ksql.execution.common.QueryRow;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.util.IteratorUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TableScanOperatorTest {
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
  private CompletableFuture<Void> shouldCancelOperations;
  @Mock
  private Row ROW1_1;
  @Mock
  private Row ROW1_2;
  @Mock
  private Row ROW3_1;
  @Mock
  private Row ROW3_2;
  @Mock
  private GenericKey GKEY11;
  @Mock
  private GenericKey GKEY12;
  @Mock
  private GenericKey GKEY31;
  @Mock
  private GenericKey GKEY32;

  @Before
  public void setUp() {
    when(ROW1_1.key()).thenReturn(GKEY11);
    when(ROW1_2.key()).thenReturn(GKEY12);
    when(ROW3_1.key()).thenReturn(GKEY31);
    when(ROW3_2.key()).thenReturn(GKEY32);
  }

  @Test
  public void shouldLookupRowsForTableScan() {
    //Given:
    final List<KsqlPartitionLocation> singleKeyPartitionLocations = new ArrayList<>();
    singleKeyPartitionLocations.add(new KsLocator.PartitionLocation(
        Optional.empty(), 1, ImmutableList.of(node1)));
    singleKeyPartitionLocations.add(new KsLocator.PartitionLocation(
        Optional.empty(), 2, ImmutableList.of(node2)));
    singleKeyPartitionLocations.add(new KsLocator.PartitionLocation(
        Optional.empty(), 3, ImmutableList.of(node3)));
    singleKeyPartitionLocations.add(new KsLocator.PartitionLocation(
        Optional.empty(), 3, ImmutableList.of(node3)));

    final TableScanOperator lookupOperator
        = new TableScanOperator(
            materialization, logicalNode, shouldCancelOperations, Optional.empty());
    when(materialization.nonWindowed()).thenReturn(nonWindowedTable);

    when(nonWindowedTable.get(1, Optional.empty())).thenReturn(
        IteratorUtil.of(ROW1_1, ROW1_2));
    when(nonWindowedTable.get(2, Optional.empty())).thenReturn(
        IteratorUtil.of());
    when(nonWindowedTable.get(3, Optional.empty())).thenReturn(
        IteratorUtil.of(ROW3_1, ROW3_2));


    lookupOperator.setPartitionLocations(singleKeyPartitionLocations);
    lookupOperator.open();

    //Then:
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY11));
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY12));
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY31));
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY32));
    assertThat(lookupOperator.next(), is(nullValue()));
    assertThat(lookupOperator.getReturnedRowCount(), is(4L));
  }

  @Test
  public void shouldCancel() {
    //Given:
    final List<KsqlPartitionLocation> singleKeyPartitionLocations = new ArrayList<>();
    singleKeyPartitionLocations.add(new KsLocator.PartitionLocation(
        Optional.empty(), 1, ImmutableList.of(node1)));
    singleKeyPartitionLocations.add(new KsLocator.PartitionLocation(
        Optional.empty(), 2, ImmutableList.of(node2)));
    singleKeyPartitionLocations.add(new KsLocator.PartitionLocation(
        Optional.empty(), 3, ImmutableList.of(node3)));
    singleKeyPartitionLocations.add(new KsLocator.PartitionLocation(
        Optional.empty(), 3, ImmutableList.of(node3)));

    final TableScanOperator lookupOperator
        = new TableScanOperator(
            materialization, logicalNode, shouldCancelOperations, Optional.empty());
    when(materialization.nonWindowed()).thenReturn(nonWindowedTable);

    when(nonWindowedTable.get(1, Optional.empty())).thenReturn(
        IteratorUtil.of(ROW1_1, ROW1_2));


    lookupOperator.setPartitionLocations(singleKeyPartitionLocations);
    lookupOperator.open();

    //Then:
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY11));
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY12));
    when(shouldCancelOperations.isDone()).thenReturn(true);
    assertThat(lookupOperator.next(), is(nullValue()));
    assertThat(lookupOperator.next(), is(nullValue()));
    assertThat(lookupOperator.getReturnedRowCount(), is(2L));
  }
}
