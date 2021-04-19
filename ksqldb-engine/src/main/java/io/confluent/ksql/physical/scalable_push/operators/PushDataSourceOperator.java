package io.confluent.ksql.physical.scalable_push.operators;

import io.confluent.ksql.physical.scalable_push.ScalablePushRegistry;

public interface PushDataSourceOperator {
  ScalablePushRegistry getScalablePushRegistry();
}
