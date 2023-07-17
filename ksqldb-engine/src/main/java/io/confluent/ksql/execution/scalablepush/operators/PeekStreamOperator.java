/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.scalablepush.operators;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.execution.common.operators.AbstractPhysicalOperator;
import io.confluent.ksql.execution.scalablepush.ProcessingQueue;
import io.confluent.ksql.execution.scalablepush.ScalablePushRegistry;
import io.confluent.ksql.execution.scalablepush.ScalablePushRegistry.CatchupMetadata;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.query.QueryId;
import java.util.List;
import java.util.Optional;

/**
 * A physical operator which utilizes a {@link ScalablePushRegistry} to register for output rows.
 * These are then fed to the upstream operators.
 */
public class PeekStreamOperator extends AbstractPhysicalOperator implements PushDataSourceOperator {

  private final DataSourceNode logicalNode;
  private final ScalablePushRegistry scalablePushRegistry;
  private final ProcessingQueue processingQueue;
  private final Optional<CatchupMetadata> catchupMetadata;
  private long rowsRead = 0;

  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public PeekStreamOperator(
      final ScalablePushRegistry scalablePushRegistry,
      final DataSourceNode logicalNode,
      final QueryId queryId,
      final Optional<CatchupMetadata> catchupMetadata
  ) {
    this.scalablePushRegistry = scalablePushRegistry;
    this.logicalNode = logicalNode;
    this.processingQueue = new ProcessingQueue(queryId);
    this.catchupMetadata = catchupMetadata;
  }

  @Override
  public void open() {
    scalablePushRegistry.register(processingQueue, catchupMetadata);
  }

  @Override
  public Object next() {
    rowsRead++;
    return processingQueue.poll();
  }

  @Override
  public void close() {
    processingQueue.close();
    scalablePushRegistry.unregister(processingQueue);
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
  public AbstractPhysicalOperator getChild(final int index) {
    return null;
  }

  @Override
  public List<AbstractPhysicalOperator> getChildren() {
    return null;
  }

  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP",
      justification = "scalablePushRegistry is meant to be exposed"
  )
  @Override
  public ScalablePushRegistry getScalablePushRegistry() {
    return scalablePushRegistry;
  }

  @Override
  public void setNewRowCallback(final Runnable newRowCallback) {
    processingQueue.setNewRowCallback(newRowCallback);
  }

  @Override
  public boolean droppedRows() {
    return processingQueue.hasDroppedRows();
  }

  @Override
  public boolean hasError() {
    return processingQueue.getHasError();
  }

  @Override
  public long getRowsReadCount() {
    return rowsRead;
  }
}
