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

package io.confluent.ksql.physical.pull.operators;

import io.confluent.ksql.execution.streams.materialization.Locator.KsqlPartitionLocation;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.execution.streams.materialization.Row;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.PlanNode;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableScanOperator extends AbstractPhysicalOperator
    implements UnaryPhysicalOperator, DataSourceOperator {

  private static final Logger LOG = LoggerFactory.getLogger(TableScanOperator.class);

  private final Materialization mat;
  private final DataSourceNode logicalNode;

  private List<KsqlPartitionLocation> partitionLocations;
  private Iterator<Row> resultIterator;
  private Iterator<KsqlPartitionLocation> partitionLocationIterator;
  private KsqlPartitionLocation nextLocation;

  public TableScanOperator(
      final Materialization mat,
      final DataSourceNode logicalNode
  ) {
    this.mat = Objects.requireNonNull(mat, "mat");
    this.logicalNode = Objects.requireNonNull(logicalNode, "logicalNode");
  }

  @Override
  public void open() {
    partitionLocationIterator = partitionLocations.iterator();
    if (partitionLocationIterator.hasNext()) {
      nextLocation = partitionLocationIterator.next();
      if (nextLocation.getKeys().isPresent()) {
        throw new IllegalStateException("Table scans should not be done with keys");
      }
      resultIterator = mat.nonWindowed()
          .get(nextLocation.getPartition());
    }
  }

  @Override
  public Object next() {
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
      resultIterator = mat.nonWindowed()
          .get(nextLocation.getPartition());
    }

    return resultIterator.next();
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
  public List<KsqlPartitionLocation> getPartitionLocations() {
    return partitionLocations;
  }

  @Override
  public void setPartitionLocations(final List<KsqlPartitionLocation> locations) {
    Objects.requireNonNull(locations, "locations");
    partitionLocations = locations;
  }
}
