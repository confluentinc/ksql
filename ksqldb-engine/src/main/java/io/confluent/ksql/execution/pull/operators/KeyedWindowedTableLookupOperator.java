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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.execution.common.QueryRowImpl;
import io.confluent.ksql.execution.common.operators.AbstractPhysicalOperator;
import io.confluent.ksql.execution.common.operators.UnaryPhysicalOperator;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlKey;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlPartitionLocation;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.execution.streams.materialization.WindowedRow;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.KeyConstraint;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.QueryFilterNode.WindowBounds;
import io.confluent.ksql.util.ConsistencyOffsetVector;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyedWindowedTableLookupOperator
    extends AbstractPhysicalOperator
    implements UnaryPhysicalOperator, DataSourceOperator {

  private static final Logger LOG = LoggerFactory.getLogger(
      KeyedWindowedTableLookupOperator.class);

  private final Materialization mat;
  private final DataSourceNode logicalNode;
  private final Optional<ConsistencyOffsetVector> consistencyOffsetVector;

  private ImmutableList<KsqlPartitionLocation> partitionLocations;
  private Iterator<WindowedRow> resultIterator;
  private Iterator<KsqlKey> keyIterator;
  private Iterator<KsqlPartitionLocation> partitionLocationIterator;
  private KsqlPartitionLocation nextLocation;
  private KsqlKey nextKey;
  private long returnedRows = 0;


  public KeyedWindowedTableLookupOperator(
      final Materialization mat,
      final DataSourceNode logicalNode,
      final Optional<ConsistencyOffsetVector> consistencyOffsetVector
  ) {
    this.logicalNode = Objects.requireNonNull(logicalNode, "logicalNode");
    this.mat = Objects.requireNonNull(mat, "mat");
    this.consistencyOffsetVector = Objects.requireNonNull(
        consistencyOffsetVector, "consistencyOffsetVector");
  }

  @Override
  public void open() {
    partitionLocationIterator = partitionLocations.iterator();
    if (partitionLocationIterator.hasNext()) {
      nextLocation = partitionLocationIterator.next();
      if (!nextLocation.getKeys().isPresent()) {
        throw new IllegalStateException("Table windowed queries should be done with keys");
      }
      keyIterator = nextLocation.getKeys().get().stream().iterator();
      if (keyIterator.hasNext()) {
        nextKey = keyIterator.next();
        updateIterator(getWindowBounds(nextKey));
      }
    }
  }

  @Override
  public Object next() {
    while (!resultIterator.hasNext()) {
      // Exhausted resultIterator
      if (!keyIterator.hasNext()) {
        if (partitionLocationIterator.hasNext()) {
          nextLocation = partitionLocationIterator.next();
        } else {
          // Exhausted all iterators
          return null;
        }
        if (!nextLocation.getKeys().isPresent()) {
          throw new IllegalStateException("Table lookup queries should be done with keys");
        }
        keyIterator = nextLocation.getKeys().get().iterator();
      }
      nextKey = keyIterator.next();
      updateIterator(getWindowBounds(nextKey));
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

  private static WindowBounds getWindowBounds(final KsqlKey ksqlKey) {
    if (!(ksqlKey instanceof KeyConstraint)) {
      throw new IllegalStateException(String.format("Table windowed queries should be done with "
          + "key constraints: %s", ksqlKey.toString()));
    }
    final KeyConstraint keyConstraintKey = (KeyConstraint) ksqlKey;
    if (!keyConstraintKey.getWindowBounds().isPresent()) {
      throw new IllegalStateException(String.format("Table windowed queries should be done with "
          + "window bounds: %s", ksqlKey));
    }
    return keyConstraintKey.getWindowBounds().get();
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
    throw new UnsupportedOperationException();
  }

  @Override
  public List<AbstractPhysicalOperator> getChildren() {
    throw new UnsupportedOperationException();
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

  private void updateIterator(final WindowBounds windowBounds) {
    resultIterator = mat.windowed().get(
        nextKey.getKey(),
        nextLocation.getPartition(),
        windowBounds.getMergedStart(),
        windowBounds.getMergedEnd(),
        consistencyOffsetVector);
  }
}
