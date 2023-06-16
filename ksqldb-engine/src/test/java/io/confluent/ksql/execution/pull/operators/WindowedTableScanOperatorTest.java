package io.confluent.ksql.execution.pull.operators;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
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
public class WindowedTableScanOperatorTest {

  @Mock
  private KsqlNode node1;
  @Mock
  private KsqlNode node2;
  @Mock
  private KsqlNode node3;
  @Mock
  private Materialization materialization;
  @Mock
  private MaterializedWindowedTable windowedTable;
  @Mock
  private DataSourceNode logicalNode;
  @Mock
  private CompletableFuture<Void> shouldCancelOperations;
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

  @Before
  public void setUp() {
    when(WINDOWED_ROW1.key()).thenReturn(GKEY1);
    when(WINDOWED_ROW2.key()).thenReturn(GKEY2);
    when(WINDOWED_ROW3.key()).thenReturn(GKEY3);
    when(WINDOWED_ROW4.key()).thenReturn(GKEY4);
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

    final WindowedTableScanOperator lookupOperator =
        new WindowedTableScanOperator(
            materialization, logicalNode, shouldCancelOperations, Optional.empty());
    when(materialization.windowed()).thenReturn(windowedTable);
    when(windowedTable.get(1, Range.all(), Range.all(), Optional.empty()))
        .thenReturn(IteratorUtil.of(WINDOWED_ROW1,WINDOWED_ROW2));
    when(windowedTable.get(2, Range.all(), Range.all(), Optional.empty()))
        .thenReturn(IteratorUtil.of());
    when(windowedTable.get(3, Range.all(), Range.all(), Optional.empty()))
        .thenReturn(IteratorUtil.of(WINDOWED_ROW3, WINDOWED_ROW2, WINDOWED_ROW4));
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

    final WindowedTableScanOperator lookupOperator =
        new WindowedTableScanOperator(
            materialization, logicalNode, shouldCancelOperations, Optional.empty());
    when(materialization.windowed()).thenReturn(windowedTable);
    when(windowedTable.get(1, Range.all(), Range.all(), Optional.empty()))
        .thenReturn(IteratorUtil.of(WINDOWED_ROW1, WINDOWED_ROW2));
    lookupOperator.setPartitionLocations(singleKeyPartitionLocations);
    lookupOperator.open();

    //Then:
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY1));
    assertThat(((QueryRow) lookupOperator.next()).key(), is(GKEY2));
    when(shouldCancelOperations.isDone()).thenReturn(true);
    assertThat(lookupOperator.next(), is(nullValue()));
    assertThat(lookupOperator.next(), is(nullValue()));
    assertThat(lookupOperator.getReturnedRowCount(), is(2L));
  }
}
