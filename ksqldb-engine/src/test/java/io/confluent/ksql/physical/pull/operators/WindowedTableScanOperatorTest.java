package io.confluent.ksql.physical.pull.operators;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlNode;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlPartitionLocation;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.execution.streams.materialization.MaterializedWindowedTable;
import io.confluent.ksql.execution.streams.materialization.WindowedRow;
import io.confluent.ksql.execution.streams.materialization.ks.KsLocator;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.util.IteratorUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
  private WindowedRow WINDOWED_ROW1;
  @Mock
  private WindowedRow WINDOWED_ROW2;
  @Mock
  private WindowedRow WINDOWED_ROW3;
  @Mock
  private WindowedRow WINDOWED_ROW4;

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
        new WindowedTableScanOperator(materialization, logicalNode);
    when(materialization.windowed()).thenReturn(windowedTable);
    when(windowedTable.get(1, Range.all(), Range.all()))
        .thenReturn(IteratorUtil.of(WINDOWED_ROW1, WINDOWED_ROW2));
    when(windowedTable.get(2, Range.all(), Range.all()))
        .thenReturn(IteratorUtil.of());
    when(windowedTable.get(3, Range.all(), Range.all()))
        .thenReturn(IteratorUtil.of(WINDOWED_ROW3, WINDOWED_ROW2, WINDOWED_ROW4));
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
