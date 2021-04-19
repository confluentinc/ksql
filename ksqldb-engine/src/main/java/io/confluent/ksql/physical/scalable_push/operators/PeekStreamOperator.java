package io.confluent.ksql.physical.scalable_push.operators;

import io.confluent.ksql.physical.pull.operators.AbstractPhysicalOperator;
import io.confluent.ksql.physical.scalable_push.ProcessingQueue;
import io.confluent.ksql.physical.scalable_push.ScalablePushRegistry;
import io.confluent.ksql.planner.plan.DataSourceNode;
import io.confluent.ksql.planner.plan.PlanNode;
import java.util.List;

public class PeekStreamOperator extends AbstractPhysicalOperator implements PushDataSourceOperator {

  private final DataSourceNode logicalNode;
  private final ScalablePushRegistry scalablePushRegistry;
  private final ProcessingQueue processingQueue;

  public PeekStreamOperator(
      final ScalablePushRegistry scalablePushRegistry,
      final DataSourceNode logicalNode
  ) {
    this.scalablePushRegistry = scalablePushRegistry;
    this.logicalNode = logicalNode;
    this.processingQueue = new ProcessingQueue();
  }

  @Override
  public void open() {
    scalablePushRegistry.register(processingQueue);
  }

  @Override
  public Object next() {
    return processingQueue.get();
  }

  @Override
  public void close() {
    processingQueue.close();
    scalablePushRegistry.unregister(processingQueue);
  }

  @Override
  public PlanNode getLogicalNode() {
    return null;
  }

  @Override
  public void addChild(AbstractPhysicalOperator child) {

  }

  @Override
  public AbstractPhysicalOperator getChild(int index) {
    return null;
  }

  @Override
  public List<AbstractPhysicalOperator> getChildren() {
    return null;
  }

  @Override
  public ScalablePushRegistry getScalablePushRegistry() {
    return scalablePushRegistry;
  }
}
