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

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.execution.streams.materialization.Locator.KsqlPartitionLocation;
import io.confluent.ksql.execution.streams.materialization.Materialization;
import io.confluent.ksql.execution.streams.materialization.WindowedRow;
import io.confluent.ksql.physical.pull.operators.WhereInfo.WindowBounds;
import io.confluent.ksql.planner.plan.DataSourceNode;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyedWindowedTableLookupOperator
    extends AbstractPhysicalOperator
    implements UnaryPhysicalOperator, DataSourceOperator {

  private static final Logger LOG = LoggerFactory.getLogger(
      KeyedWindowedTableLookupOperator.class);

  private final Materialization mat;
  private final DataSourceNode logicalOperator;
  private final WindowBounds windowBounds;

  private List<KsqlPartitionLocation> partitionLocations;
  private Iterator<WindowedRow> resultIterator;
  private Iterator<GenericKey> keyIterator;
  private Iterator<KsqlPartitionLocation> partitionLocationIterator;
  private KsqlPartitionLocation nextLocation;
  private GenericKey nextKey;


  public KeyedWindowedTableLookupOperator(
      final Materialization mat,
      final DataSourceNode logicalNode,
      final WindowBounds windowBounds
  ) {
    this.logicalOperator = Objects.requireNonNull(logicalNode, "logicalNode");
    this.mat = Objects.requireNonNull(mat, "mat");
    this.windowBounds = Objects.requireNonNull(windowBounds, "windowBounds");
  }

  @Override
  public void open() {
    partitionLocationIterator = partitionLocations.iterator();
    if (partitionLocationIterator.hasNext()) {
      nextLocation = partitionLocationIterator.next();
      if (!nextLocation.getKeys().isPresent()) {
        throw new IllegalStateException("Table windowed queries should be done with keys");
      }
      keyIterator = nextLocation.getKeys().get().iterator();
      if (keyIterator.hasNext()) {
        nextKey = keyIterator.next();
        resultIterator = mat.windowed().get(
            nextKey,
            nextLocation.getPartition(),
            windowBounds.getStart(),
            windowBounds.getEnd())
            .iterator();
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
      resultIterator = mat.windowed().get(
          nextKey,
          nextLocation.getPartition(),
          windowBounds.getStart(),
          windowBounds.getEnd())
          .iterator();
    }
    return resultIterator.next();

  }

  @Override
  public void close() {

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
  public List<KsqlPartitionLocation> getPartitionLocations() {
    return partitionLocations;
  }

  @Override
  public void setPartitionLocations(final List<KsqlPartitionLocation> locations) {
    Objects.requireNonNull(locations, "locations");
    partitionLocations = locations;
  }
}
