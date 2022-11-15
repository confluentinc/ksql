/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.execution.common.QueryRowImpl;
import io.confluent.ksql.execution.common.operators.AbstractPhysicalOperator;
import io.confluent.ksql.execution.common.operators.UnaryPhysicalOperator;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlPartitionLocation;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.execution.streams.materialization.WindowedRow;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.util.ConsistencyOffsetVector;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WindowedTableScanOperator extends AbstractPhysicalOperator
    implements UnaryPhysicalOperator, DataSourceOperator {

  private static final Logger LOG = LoggerFactory.getLogger(TableScanOperator.class);

  private final Materialization mat;
  private final DataSourceNode logicalNode;
  private final CompletableFuture<Void> shouldCancelOperations;
  private final Optional<ConsistencyOffsetVector> consistencyOffsetVector;

  private ImmutableList<KsqlPartitionLocation> partitionLocations;
  private Iterator<WindowedRow> resultIterator;
  private Iterator<KsqlPartitionLocation> partitionLocationIterator;
  private KsqlPartitionLocation nextLocation;
  private long returnedRows = 0;

  public WindowedTableScanOperator(
      final Materialization mat,
      final DataSourceNode logicalNode,
      final CompletableFuture<Void> shouldCancelOperations,
      final Optional<ConsistencyOffsetVector> consistencyOffsetVector
  ) {
    this.mat = Objects.requireNonNull(mat, "mat");
    this.logicalNode = Objects.requireNonNull(logicalNode, "logicalNode");
    this.shouldCancelOperations =  Objects.requireNonNull(shouldCancelOperations,
        "shouldCancelOperations");
    this.consistencyOffsetVector = Objects.requireNonNull(
        consistencyOffsetVector, "consistencyOffsetVector");
  }

  @Override
  public void open() {
    partitionLocationIterator = partitionLocations.iterator();
    if (partitionLocationIterator.hasNext()) {
      nextLocation = partitionLocationIterator.next();
      if (nextLocation.getKeys().isPresent()) {
        throw new IllegalStateException("Table scans should not be done with keys");
      }
      updateIterator();
    }
  }

  @Override
  public Object next() {
    if (shouldCancelOperations.isDone()) {
      return null;
    }

    while (!resultIterator.hasNext()) {
      // Exhausted resultIterator
      if (partitionLocationIterator.hasNext()) {
        nextLocation = partitionLocationIterator.next();
      } else {
        // Exhausted all iterators
        return null;
      }
      if (nextLocation.getKeys().isPresent()) {
        throw new IllegalStateException("Table scans should not be done with keys");
      }
      updateIterator();
    }

    returnedRows++;
    final WindowedRow row = resultIterator.next();
    return QueryRowImpl.of(
        row.schema(),
        row.key(),
        row.window(),
        row.value(),
        row.rowTime()
    );
  }

  @Override
  public void close() {

  }

  @Override
  public PlanNode getLogicalNode() {
    return logicalNode;
  }

  @Override
  public void addChild(final AbstractPhysicalOperator child) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AbstractPhysicalOperator getChild() {
    return null;
  }

  @Override
  public AbstractPhysicalOperator getChild(final int index) {
    return null;
  }

  @Override
  public List<AbstractPhysicalOperator> getChildren() {
    return null;
  }

  @Override
  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP",
      justification = "partitionLocations is ImmutableList"
  )
  public List<KsqlPartitionLocation> getPartitionLocations() {
    return partitionLocations;
  }

  @Override
  public void setPartitionLocations(final List<KsqlPartitionLocation> locations) {
    Objects.requireNonNull(locations, "locations");
    partitionLocations = ImmutableList.copyOf(locations);
  }

  @Override
  public long getReturnedRowCount() {
    return returnedRows;
  }

  private void updateIterator() {
    resultIterator = mat.windowed()
        .get(nextLocation.getPartition(), Range.all(), Range.all(), consistencyOffsetVector);
  }
}
